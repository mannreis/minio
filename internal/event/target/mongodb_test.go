package target

import (
	"context"
	"testing"

	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/logger"
)

type fields struct {
	Enable     bool
	ConnString string
	Database   string
	Collection string
	Format     string
}

func (f *fields) ToArgs() MongoDBArgs {
	return MongoDBArgs{Enable: f.Enable, ConnectionString: f.ConnString, Database: f.Database, Collection: f.Collection, Format: f.Format}
}

func logOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "notify-mongo-tests", err, id, errKind...)
}

func TestMongoDBTarget(t *testing.T) {
	mongoTarget, err := NewMongoDBTarget(t.Context(), "test_mongo_target", MongoDBArgs{}, logOnceIf, nil)
	if err != nil {
		t.Fatalf("NewMongoDBTarget should not fail with empty MongoDBArgs")
	}
	if mongoTarget.mongo.client != nil {
		t.Fatalf("MongoDB target creation should not estabilsh a connection!")
	}
}

func TestMongoDBArgs(t *testing.T) {
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name:    "empty_enabled",
			fields:  fields{Enable: true},
			wantErr: true,
		},
		{
			name:    "empty_disabled",
			fields:  fields{Enable: false},
			wantErr: false,
		},
		{
			name: "bad_connection_string",
			fields: fields{
				Enable:     true,
				ConnString: "Invalid connection string",
			},
			wantErr: true,
		},
		{
			name: "no_collection",
			fields: fields{
				Enable:     true,
				ConnString: "mongodb://mocked:4321",
				Database:   "minio", // Collection required
			},
			wantErr: true,
		},
		{
			name: "naming_conflict",
			fields: fields{
				Enable:     true,
				Database:   "events",
				Collection: "events", // Cannot be same as database
				Format:     "raw",
			},
			wantErr: true,
		},
		{
			name: "bad_format",
			fields: fields{
				Enable:     true,
				ConnString: "mongodb://test:214",
				Database:   "minio-mongo",
				Collection: "raw_format",
				Format:     "unknown",
			},
			wantErr: true,
		},
		{
			name: "ok_raw",
			fields: fields{
				Enable:     true,
				ConnString: "mongodb://test:214",
				Database:   "minio-mongo",
				Collection: "raw_format",
				Format:     "raw",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.fields.ToArgs()
			if err := n.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("MongoDBArgs.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMongoFormatDocument(t *testing.T) {
	dummyEvent := event.Event{
		EventVersion:      "2.0",
		EventSource:       "minio:s3",
		AwsRegion:         "region",
		EventTime:         event.AMZTimeFormat,
		EventName:         event.ObjectCreatedCompleteMultipartUpload,
		UserIdentity:      event.Identity{PrincipalID: "principalId"},
		RequestParameters: map[string]string{"head1": "val"}, // Element order on bson vs json might differ
		ResponseElements:  map[string]string{"resp1": "val"}, // Element order on bson vs json might differ
		S3: event.Metadata{
			SchemaVersion:   "1.0",
			ConfigurationID: "Config",
			Bucket: event.Bucket{
				Name:          "bucketname",
				OwnerIdentity: event.Identity{PrincipalID: "principalId"},
				ARN:           "arn:::bucketname",
			},
			Object: event.Object{
				Key:       "key",
				VersionID: "versionid",
				Sequencer: "sequencer",
			},
		},
		Source: event.Source{
			Host:      "useragent",
			UserAgent: "host",
		},
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "ok_raw",
			fields: fields{
				Enable:     true,
				ConnString: "mongodb://test:214",
				Database:   "minio-mongo",
				Collection: "raw_format",
				Format:     "raw",
			},
			wantErr: false,
		},
		{
			name: "ok_namespace",
			fields: fields{
				Enable:     true,
				ConnString: "mongodb://test:214",
				Database:   "minio-mongo",
				Collection: "namespace_format",
				Format:     "namespace",
			},
			wantErr: false,
		},
		{
			name: "ok_access",
			fields: fields{
				Enable:     true,
				ConnString: "mongodb://test:214",
				Database:   "minio-mongo",
				Collection: "access_format",
				Format:     "access",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := tt.fields.ToArgs()
			if err := args.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("MongoDBArgs.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			target, err := NewMongoDBTarget(t.Context(), "test_format_raw", args, logOnceIf, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewMongoDBTarget() error = %v, wantErr %v", err, tt.wantErr)
			}
			doc, err := target.toFormatDocument(dummyEvent)
			if (err != nil) != tt.wantErr {
				t.Errorf("MongoDBTarget.toFormatDocument() error = %v, wantErr %v", err, tt.wantErr)
			}
			if doc == nil {
				t.Errorf("document should not be nil")
			}
			t.Logf("%v\n", doc)
		})
	}
}
