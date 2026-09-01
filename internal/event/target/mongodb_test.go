package target

import (
	"context"
	"testing"

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
	if mongoTarget.mongoClient != nil {
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
