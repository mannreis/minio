package target

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio/internal/event"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func setupMongo(t *testing.T) *mongo.Client {
	t.Helper()
	ctx := context.Background()

	ctr, err := mongodb.Run(ctx, "mongo:7")
	if err != nil {
		t.Fatalf("start container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			t.Logf("terminate container: %v", err)
		}
	})

	uri, err := ctr.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	return client
}

type constructor func(*mongo.Client, string, string) collectionWriter

func asConstructor[T collectionWriter](
	f func(*mongo.Client, string, string) T,
) constructor {
	return func(c *mongo.Client, a, b string) collectionWriter {
		return f(c, a, b)
	}
}

func TestCollectionWriterIndexes(t *testing.T) {
	tests := []struct {
		name        string
		indexCount  int // If an index is created then the default _id_ is also
		constructor constructor
	}{
		{"raw", 0, asConstructor(newRawCollectionWriter)},
		{"namespace", 1 + 2, asConstructor(newNamespaceCollectionWriter)},
		{"access", 1 + 1, asConstructor(newAccessCollectionWriter)},
	}

	client := setupMongo(t)
	for i, test := range tests {
		dbName := "test_indexes"
		collection := client.Database(dbName).Collection(test.name)
		w := test.constructor(client, dbName, test.name)
		if err := w.EnsureIndexes(); err != nil {
			t.Fatalf("[test %d - %s] EnsureIndexes: %v", i, test.name, err)
		}
		// Confirm no indexes were created.
		cur, err := collection.Indexes().List(context.Background())
		if err != nil {
			t.Fatalf("[test %d - %s] list indexes: %v", i, test.name, err)
		}
		var idx []bson.M
		if err := cur.All(context.Background(), &idx); err != nil {
			t.Fatalf("[test %d - %s] decode indexes: %v", i, test.name, err)
		}
		if len(idx) != test.indexCount {
			t.Fatalf("[test %d - %s] expected %d indexes, got %d: %+v", i, test.name, test.indexCount, len(idx), idx)
		}
	}
}

type testDocsArgs struct {
	docs          []event.Event
	expectedCount map[string]int
}

func helperTestCollectionWriter(t *testing.T, client *mongo.Client, dbName string, args testDocsArgs) {
	collectionWriters := []struct {
		name        string
		constructor constructor
	}{
		{"raw", asConstructor(newRawCollectionWriter)},
		{"namespace", asConstructor(newNamespaceCollectionWriter)},
		{"access", asConstructor(newAccessCollectionWriter)},
	}

	for i, test := range collectionWriters {
		collection := client.Database(dbName).Collection(test.name)
		w := test.constructor(client, dbName, test.name)
		if err := w.EnsureIndexes(); err != nil {
			t.Fatalf("[test %d - %s] EnsureIndexes: %v", i, test.name, err)
		}
		if count, err := w.WriteDocuments(args.docs); err != nil {
			t.Fatalf("[test %d - %s] WriteDocuments: %v", i, test.name, err)
		} else if count < args.expectedCount[test.name] {
			// We can always write more documents then expected, just means overwrites/upsert
			t.Fatalf("[test %d - %s] document write mismatch, expected %d, got %d: %v", i, test.name, args.expectedCount[test.name], count, err)
		}

		cur, err := collection.Find(context.Background(), bson.D{})
		if err != nil {
			t.Fatalf("[test %d - %s] cannort find written documents: %v", i, test.name, err)
		}
		var results []bson.M
		if err := cur.All(context.Background(), &results); err != nil {
			t.Fatalf("[test %d - %s] error Collection.Find(): %v", i, test.name, err)
		}
		if len(results) != args.expectedCount[test.name] {
			t.Fatalf("[test %d - %s] expected %d documents, got %d: %+v", i, test.name, args.expectedCount[test.name], len(results), results)
		}
		t.Logf("Results (%d) on datase %s for collection %s: %+v\n", len(results), dbName, test.name, results)
	}
}

func TestCollectionWriterEmptyDocuments(t *testing.T) {
	docs := []event.Event{}
	client := setupMongo(t)
	helperTestCollectionWriter(t, client, "test_empty_docs", testDocsArgs{docs, map[string]int{"raw": 0, "namespace": 0, "access": 0}})
}

// testEvent builds a minimal, valid event.Event for a given bucket/key/sequencer.
// eventTime uses event.AMZTimeFormat since that's the format the rest of the
// codebase (and Mongo comparisons in namespaceCollectionWriter) expect.
func testEvent(bucket, key, sequencer string, eventTime time.Time) event.Event {
	return event.Event{
		EventVersion: "2.0",
		EventSource:  "minio:s3",
		EventTime:    eventTime.UTC().Format(event.AMZTimeFormat),
		EventName:    event.ObjectCreatedPut,
		S3: event.Metadata{
			Bucket: event.Bucket{Name: bucket},
			Object: event.Object{Key: key, Sequencer: sequencer},
		},
	}
}

func TestCollectionWriterUnorderedClashingDocuments(t *testing.T) {
	dbName := "test_all_unique_docs"
	tic := time.Now()
	d, err := time.ParseDuration("1s")
	if err != nil {
		t.Fatalf("error parsing duration: %+v", err)
	}
	args := testDocsArgs{
		docs: []event.Event{
			testEvent("bucket1", "prefix/to/object1", strings.Repeat("cafe", 4), tic),
			testEvent("bucket1", "prefix/to/object1", strings.Repeat("caff", 4), tic.Add(d*2)),
			testEvent("bucket1", "prefix/to/object1", strings.Repeat("aaaa", 4), tic.Add(-d*2)),
		},
		expectedCount: map[string]int{"raw": 3, "namespace": 1, "access": 3},
	}
	client := setupMongo(t)
	helperTestCollectionWriter(t, client, dbName, args)

	// Confirm final expected namespace event (highest event sequencer or event time)
	cur, err := client.Database(dbName).Collection("namespace").Find(t.Context(), args.docs[1])
	if err != nil {
		t.Fatalf("Unable to find expected namespace event in collection: %+v", args.docs[1])
	}

	var events []event.Event
	if err := cur.All(t.Context(), &events); err != nil {
		t.Fatalf("Unable to extract events from collection cursor: %v", err)
	}
}
