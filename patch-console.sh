breaking_v='v1.7.7-0.20250905210349-2017f33b26e1'
working_v='v1.7.6'
cp -pfr "$(go env GOMODCACHE)/github.com/minio/console@${breaking_v}/" console
# Copy deleted files/modules from workgin version
chmod -R +w console
cp -pfrv "$(go env GOMODCACHE)/github.com/minio/console@${working_v}/"api/client.go console/api/
cp -pfrv "$(go env GOMODCACHE)/github.com/minio/console@${working_v}/"pkg/auth/ldap* console/pkg/auth/
chmod -R +w console
