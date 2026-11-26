package target

import (
	"context"
	"time"

	"github.com/minio/minio/internal/event"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// -- Mongo Collection Writer for each type of document layout
type collectionWriter interface {
	EnsureIndexes() error
	WriteDocuments([]event.Event) (int, error)
}

type rawCollectionWriter struct {
	client     *mongo.Client
	collection *mongo.Collection
}

type accessCollectionWriter struct {
	rawCollectionWriter
	indexes []mongo.IndexModel
}

type namespaceCollectionWriter struct {
	rawCollectionWriter
	indexes []mongo.IndexModel
}

var (
	_ collectionWriter = (*rawCollectionWriter)(nil)
	_ collectionWriter = (*accessCollectionWriter)(nil)
	_ collectionWriter = (*namespaceCollectionWriter)(nil)
)

func newRawCollectionWriter(client *mongo.Client, dbName, collName string) *rawCollectionWriter {
	return &rawCollectionWriter{
		client:     client,
		collection: client.Database(dbName).Collection(collName),
	}
}

func (r *rawCollectionWriter) EnsureIndexes() error {
	return nil
}

func (r *rawCollectionWriter) WriteDocuments(events []event.Event) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}

	if r, err := r.collection.InsertMany(context.TODO(), events, options.InsertMany().SetOrdered(false)); err != nil {
		return len(r.InsertedIDs), err
	}
	return len(events), nil
}

// Access (event time) document format

func newAccessCollectionWriter(client *mongo.Client, dbName, collName string) *accessCollectionWriter {
	return &accessCollectionWriter{
		rawCollectionWriter: rawCollectionWriter{
			client:     client,
			collection: client.Database(dbName).Collection(collName),
		},
		indexes: []mongo.IndexModel{{
			Keys: bson.D{{Key: "eventTime", Value: 1}},
		}},
	}
}

func (a *accessCollectionWriter) EnsureIndexes() error {
	if len(a.indexes) == 0 {
		return nil
	}
	_, err := a.collection.Indexes().CreateMany(context.TODO(), a.indexes)
	return err
}

func (a *accessCollectionWriter) WriteDocuments(events []event.Event) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}

	if r, err := a.collection.InsertMany(context.TODO(), events, options.InsertMany().SetOrdered(false)); err != nil {
		return len(r.InsertedIDs), err
	}
	return len(events), nil
}

// Namespace document format

func newNamespaceCollectionWriter(client *mongo.Client, dbName, collName string) *namespaceCollectionWriter {
	return &namespaceCollectionWriter{
		rawCollectionWriter: rawCollectionWriter{
			client:     client,
			collection: client.Database(dbName).Collection(collName),
		},
		indexes: []mongo.IndexModel{{
			Keys: bson.D{
				{Key: "s3.bucket.name", Value: 1},
				{Key: "s3.object.key", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		}, {
			Keys: bson.D{
				{Key: "s3.object.sequencer", Value: 1},
			},
		}},
	}
}

func (r *namespaceCollectionWriter) eventToWriteModel(eventData event.Event) mongo.WriteModel {
	filter := bson.D{
		{Key: "s3.bucket.name", Value: eventData.S3.Bucket.Name},
		{Key: "s3.object.key", Value: eventData.S3.Object.Key},
	}

	storedSequencer := bson.D{{
		Key: "$ifNull",
		Value: bson.A{
			"$s3.object.sequencer",
			"",
		},
	}}

	storedEventTime := bson.D{{
		Key: "$ifNull",
		Value: bson.A{
			"$eventTime",
			time.Time{},
		},
	}}

	incomingSequencer := eventData.S3.Object.Sequencer
	incomingEventTime := eventData.EventTime

	sequencerIsNewer := bson.D{{
		Key: "$gt",
		Value: bson.A{
			incomingSequencer,
			storedSequencer,
		},
	}}

	sequencerIsSame := bson.D{{
		Key: "$eq",
		Value: bson.A{
			incomingSequencer,
			storedSequencer,
		},
	}}

	eventTimeIsNewer := bson.D{{
		Key: "$gt",
		Value: bson.A{
			incomingEventTime,
			storedEventTime,
		},
	}}

	incomingEventIsNewer := bson.D{{
		Key: "$or",
		Value: bson.A{
			// Newer sequencer
			sequencerIsNewer,

			// Same sequencer, but newer event time
			bson.D{{
				Key: "$and",
				Value: bson.A{
					sequencerIsSame,
					eventTimeIsNewer,
				},
			}},
		},
	}}

	update := mongo.Pipeline{
		bson.D{{
			Key: "$replaceWith",
			Value: bson.D{{
				Key: "$cond",
				Value: bson.A{
					incomingEventIsNewer,

					// then: replace the whole document
					eventData,

					// else: leave the document untouched
					"$$ROOT",
				},
			}},
		}},
	}
	return mongo.NewUpdateOneModel().
		SetFilter(filter).
		SetUpdate(update).
		SetUpsert(true)
}

func (r *namespaceCollectionWriter) EnsureIndexes() error {
	if len(r.indexes) == 0 {
		return nil
	}
	_, err := r.collection.Indexes().CreateMany(context.TODO(), r.indexes)
	return err
}

func (r *namespaceCollectionWriter) WriteDocuments(events []event.Event) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}

	models := make([]mongo.WriteModel, 0, len(events))
	for _, evt := range events {
		models = append(models, r.eventToWriteModel(evt))
	}

	result, err := r.collection.BulkWrite(context.TODO(), models, options.BulkWrite().SetOrdered(false))

	return int(result.UpsertedCount + result.InsertedCount + result.ModifiedCount), err
}
