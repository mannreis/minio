package event

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func diffSideBySide(a, b []byte) (string, error) {
	cmd := exec.Command("diff", "-y", "/dev/fd/3", "/dev/fd/4")

	r1, w1, err := os.Pipe()
	if err != nil {
		return "", err
	}
	defer r1.Close()

	r2, w2, err := os.Pipe()
	if err != nil {
		return "", err
	}
	defer r2.Close()

	cmd.ExtraFiles = []*os.File{r1, r2}

	go func() {
		_, _ = w1.Write(a)
		_ = w1.Close()
	}()

	go func() {
		_, _ = w2.Write(b)
		_ = w2.Close()
	}()

	out, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); !ok {
			return "", err
		}
	}

	return string(out), nil
}

func jsonIndent(b []byte) []byte {
	var buf bytes.Buffer
	if err := json.Indent(&buf, b, "", "  "); err != nil {
		return nil
	}
	return buf.Bytes()
}

func marshalExtJSONWithJSONTags(val interface{}, canonical, escapeHTML bool) ([]byte, error) {
	var buf bytes.Buffer

	vw := bson.NewExtJSONValueWriter(&buf, canonical, escapeHTML)
	enc := bson.NewEncoder(vw)
	enc.UseJSONStructTags()
	if err := enc.Encode(val); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func TestEventBSONtoJSONMarshal(t *testing.T) {
	testCases := []struct {
		event     Event
		expectErr bool
	}{
		{Event{}, false},
		{Event{
			EventVersion:      "2.0",
			EventSource:       "minio:s3",
			AwsRegion:         "region",
			EventTime:         AMZTimeFormat,
			EventName:         ObjectCreatedCompleteMultipartUpload,
			UserIdentity:      Identity{PrincipalID: "principalId"},
			RequestParameters: map[string]string{"head1": "val"}, // Element order on bson vs json might differ
			ResponseElements:  map[string]string{"resp1": "val"}, // Element order on bson vs json might differ
			S3: Metadata{
				SchemaVersion:   "1.0",
				ConfigurationID: "Config",
				Bucket: Bucket{
					Name:          "bucketname",
					OwnerIdentity: Identity{PrincipalID: "principalId"},
					ARN:           "arn:::bucketname",
				},
				Object: Object{
					Key:       "key",
					VersionID: "versionid",
					Sequencer: "sequencer",
				},
			},
			Source: Source{
				Host:      "useragent",
				UserAgent: "host",
			},
		}, false},
	}
	for i, testCase := range testCases {
		bsonbytes, err := marshalExtJSONWithJSONTags(testCase.event, true, false)
		expectErr := (err != nil)
		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		var event Event
		json.Unmarshal(bsonbytes, &event)

		jsonbytes, err := json.Marshal(testCase.event)
		expectErr = (err != nil)
		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		jsonInd := jsonIndent(jsonbytes)
		if jsonInd == nil {
			t.Fatalf("test: %v: eror: cannot indent bytes as json (json)", i+1)
		}
		bsonInd := jsonIndent(bsonbytes)
		if bsonInd == nil {
			t.Fatalf("test: %v: eror: cannot indent bytes as json (bson)", i+1)
		}

		if !reflect.DeepEqual(jsonInd, bsonInd) {
			diff, err := diffSideBySide(jsonInd, bsonInd)
			if err != nil {
				t.Logf("Failed to run diff %s\n", err)
			}
			t.Fatalf("test %v: bytes: mismatch: %v vs %v: \n\t%s", i+1, string(jsonInd), string(bsonInd), diff)
		}
	}
}
