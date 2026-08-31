#!/usr/bin/env bash
set -euox pipefail

CLUSTERS=( # to match docker-compose.yaml
    "http://0.0.0.0:11000"
    "http://0.0.0.0:12000"
)

ADMIN_ALIAS=tmp-admin-docker
ADMIN_ALIASES=()
USER_ALIAS='tmp-minio'
USER_ALIASES=()
BUCKET_NAME='bucket'
BUCKETS=()
for n in "${!CLUSTERS[@]}"; do
	ADMIN_ALIASES+=("${ADMIN_ALIAS}-$(( n+1 ))")
	USER_ALIASES+=("${USER_ALIAS}-$(( n+1 ))")
	BUCKETS+=("${BUCKET_NAME}-$(( n+1 ))")
done

DOCKER_CACHE=1

declare -A NOTIFICATION_TARGETS
NOTIFICATION_TARGETS[mongo]='notify_mongodb:docker enable=on connection_string="mongodb://172.28.0.13:27017" database="minio" collection=raw' #queue_dir= queue_limit= batch_size batch_timeout client_cert client_key=

ARNS=()
for t in "${NOTIFICATION_TARGETS[@]}"; do
	notification=$(echo "${t}" |grep -oP 'notify_\K(\w+:\w+)')
	IFS=: read -r type name <<<"$notification"
	ARNS+=("arn:minio:sqs::${name}:${type}")
done

function cleanup() {
	echo "Cleaning up MinIO deployment"
	docker-compose -f "${DOCKER_COMPOSE_FILE}" down --volumes
	for container in $(docker ps -q); do
		echo Removing docker "${container}"
		docker rm -f "${container}" >/dev/null 2>&1
		docker wait "${container}"
	done
	if [ ${DOCKER_CACHE} -ne 1 ]; then
		prune
	fi
}

function prune() {
	docker system prune --volumes --force
	docker image prune --all --force
}

function join_by {
  local d=${1-} f=${2-}
  if shift 2; then
    printf %s "$f" "${@/#/$d}"
  fi
}

function test_bucket_creation_spread_across_clusters() {
	echo
	echo -e "Running test_bucket_creation_spread_across_clusters ..."

	for i in "${!CLUSTERS[@]}"; do
		local aliasbucket="${USER_ALIASES[$i]}/${BUCKETS[$i]}"
		./mc mb -p "${aliasbucket}"
		echo "Hi $(( i+1 ))!" | ./mc pipe "${aliasbucket}/obj${i}.txt"
		STATUS=$?
		if [ $STATUS -ne 0 ]; then
			echo -e "Could write data into bucket on cluster: ${CLUSTERS[$i]} (target: ${aliasbucket})"
			echo -e "Failed"
			return 1
		fi
	done

	for i in "${!CLUSTERS[@]}"; do
		local alias="${USER_ALIASES[$i]}"
		if test "$( mc ls "${alias}/" | grep -oP "$(join_by '|' ${BUCKETS[@]} )" | wc -l )" != "${#BUCKETS[@]}" ; then 
			echo -e "Bucket creation not spread across instances"
			echo -e "Failed"
			return 2
		fi

		# for each cluster, verify all objects are reachable from others
		for j in "${!CLUSTERS[@]}"; do
			local target="${USER_ALIASES[$i]}/${BUCKETS[$j]}"
			if test "$(mc cat "${target}/obj${j}.txt")" != "Hi $(( j + 1 ))!"; then 
				echo -e "Object creation not observed across instances"
				echo -e "Failed"
				return 3
			fi
		done
	done
}

function setup_clusters {
    echo "sleep to wait for MinIO Server to be ready prior mc commands"

	for n in "${!CLUSTERS[@]}"; do
		local alias="${USER_ALIASES[$n]}"
		local admin_alias="${ADMIN_ALIASES[$n]}"
		local cluster="${CLUSTERS[$n]}"
		TIMEOUT=10    
		while true; do
			if [[ ${TIMEOUT} -le 0 ]]; then
				echo retry: timeout while running: mc alias set
				return 1
			fi
			eval ./mc alias set "${admin_alias}" "${cluster}" minioadmin minioadmin && break
			TIMEOUT=$((TIMEOUT - 1))
			sleep 1
		done

    	./mc ready "${admin_alias}"

        # Admin setup for each MinIO instance
        ./mc admin user add "${admin_alias}" "testuser${n}" "passuser${n}"
        ./mc admin policy attach "${admin_alias}" readwrite --user "testuser${n}"

        # User setup for each MinIO instance
        ./mc alias set "${alias}" "${cluster}" "testuser${n}" "passuser${n}"
        ./mc ready "${alias}"
		
		setup_targets_alias "${admin_alias}"
done

	# default alias!?
	mc alias set ${ADMIN_ALIAS} "${CLUSTERS[0]}" minioadmin minioadmin
    ./mc ready "${ADMIN_ALIAS}"
}

function setup_targets_alias() {
	local alias="${1}"
	# Setup notification targets
	for t in "${!NOTIFICATION_TARGETS[@]}"; do
		# shellcheck disable=SC2086,SC2090
		mc admin config set "${alias}" ${NOTIFICATION_TARGETS[$t]}
	done
	mc admin service restart "${alias}"
    return  0
}

function setup_buckets_with_events(){
	for n in "${!CLUSTERS[@]}"; do
		local bucket="${USER_ALIASES[$n]}/${BUCKETS[$n]}"
		mc mb "${bucket}"
		for arn in "${ARNS[@]}"; do
			mc event add "${bucket}" "${arn}"
		done
	done
}

function main() {
    if mc --version; then
		echo 'Minio client not found, update PATH or install it: https://dl.minio.io/client/mc/release/linux-amd64/mc' >&2
	fi

	cleanup

	# Setup containers to run federated Minio + Mongo
	docker-compose -f "${DOCKER_COMPOSE_FILE}" up -d

	#trap "cleanup" ERR
    
    setup_clusters
	setup_buckets_with_events

	set -e
	test_bucket_creation_spread_across_clusters


	#cleanup
    return 0
}

main "$@"
