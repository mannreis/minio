package target

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/once"
	"github.com/minio/minio/internal/store"
	xnet "github.com/minio/pkg/v3/net"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	defaultCollectionName = "raw_events"
)

// MongoDB constants
const (
	MongoDBConnectionString = "connection_string"
	MongoDBDatabase         = "database"
	MongoDBCollection       = "collection"
	MongoDBAuthToken        = "auth_token"
	MongoDBQueueDir         = "queue_dir"
	MongoDBQueueLimit       = "queue_limit"
	MongoDBBatchSize        = "batch_size"
	MongoDBBatchTimeout     = "batch_timeout"
	MongoDBClientCert       = "client_cert"
	MongoDBClientKey        = "client_key"

	EnvMongoDBEnable           = "MINIO_NOTIFY_MONGODB_ENABLE"
	EnvMongoDBConnectionString = "MINIO_NOTIFY_MONGODB_CONNECTION_STRING"
	EnvMongoDBDatabase         = "MINIO_NOTIFY_MONGODB_DATABASE"
	EnvMongoDBCollection       = "MINIO_NOTIFY_MONGODB_COLLECTION"
	EnvMongoDBAuthToken        = "MINIO_NOTIFY_MONGODB_AUTH_TOKEN"
	EnvMongoDBQueueDir         = "MINIO_NOTIFY_MONGODB_QUEUE_DIR"
	EnvMongoDBQueueLimit       = "MINIO_NOTIFY_MONGODB_QUEUE_LIMIT"
	EnvMongoDBBatchSize        = "MINIO_NOTIFY_MONGODB_BATCH_SIZE"
	EnvMongoDBBatchTimeout     = "MINIO_NOTIFY_MONGODB_BATCH_TIMEOUT"
	EnvMongoDBClientCert       = "MINIO_NOTIFY_MONGODB_CLIENT_CERT"
	EnvMongoDBClientKey        = "MINIO_NOTIFY_MONGODB_CLIENT_KEY"
)

// MongoDBArgs - MongoDB target arguments.
type MongoDBArgs struct {
	Enable           bool            `json:"enable"`
	ConnectionString string          `json:"connectionString"` // required
	Database         string          `json:"database"`         // required
	Collection       string          `json:"collection"`       // default: "raw_events"
	Transport        *http.Transport `json:"-"`
	AuthToken        string          `json:"authToken"`
	QueueDir         string          `json:"queueDir"`
	QueueLimit       uint64          `json:"queueLimit"`
	BatchSize        uint64          `json:"batchSize"`
	BatchTimeout     time.Duration   `json:"batchTimeout"`
	ClientCert       string          `json:"clientCert"`
	ClientKey        string          `json:"clientKey"`
}

func (m MongoDBArgs) toClientOptions() (*options.ClientOptions, error) {
	bsonOpts := &options.BSONOptions{
		UseJSONStructTags: true,
	}
	opts := options.Client().
		ApplyURI(m.ConnectionString).
		SetServerSelectionTimeout(1 * time.Second).
		SetRetryWrites(true).
		SetHeartbeatInterval(10 * time.Second).
		SetBSONOptions(bsonOpts)
	//	.SetTLSConfig(transport.TLSClientConfig)

	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return opts, nil
}

// Validate MongoDBArgs fields
func (m MongoDBArgs) Validate() error {
	if !m.Enable {
		return nil
	}
	if m.ConnectionString == "" {
		return errors.New("connection string not provided")
	}

	if _, err := m.toClientOptions(); err != nil {
		return err
	}

	switch m.Database {
	case "":
		return errors.New("database name not provided")
	case m.Collection:
		return errors.New("database and collection must not have the same name")
	}

	if m.Collection == "" {
		return errors.New("Collection name cannot be empty")
	}

	if m.QueueDir != "" {
		if !filepath.IsAbs(m.QueueDir) {
			return errors.New("queueDir path should be absolute")
		}
	}
	if m.ClientCert != "" && m.ClientKey == "" || m.ClientCert == "" && m.ClientKey != "" {
		return errors.New("cert and key must be specified as a pair")
	}
	if m.BatchSize > 1 {
		if m.QueueDir == "" {
			return errors.New("batch should be enabled only if queue dir is enabled")
		}
	}
	if m.BatchTimeout > 0 {
		if m.QueueDir == "" || m.BatchSize <= 1 {
			return errors.New("batch commit timeout should be set only if queue dir is enabled and batch size > 1")
		}
	}
	return nil
}

// MongoDBTarget - MongoDB target.
type MongoDBTarget struct {
	initOnce once.Init

	id          event.TargetID
	args        MongoDBArgs
	mongoClient *mongo.Client
	store       store.Store[event.Event]
	batch       *store.Batch[event.Event]
	loggerOnce  logger.LogOnce
	cancel      context.CancelFunc
	cancelCh    <-chan struct{}
}

// ID - returns target ID.
func (target *MongoDBTarget) ID() event.TargetID {
	return target.id
}

// Name - returns the Name of the target.
func (target *MongoDBTarget) Name() string {
	return target.ID().String()
}

// IsActive - Return true if target is up and active
func (target *MongoDBTarget) IsActive() (bool, error) {
	if err := target.init(); err != nil {
		return false, err
	}
	return target.isActive()
}

// Store returns any underlying store if set.
func (target *MongoDBTarget) Store() event.TargetStore {
	return target.store
}

func (target *MongoDBTarget) isActive() (bool, error) {
	if err := target.mongoClient.Ping(context.TODO(), nil); err != nil {
		return false, err
	}
	return true, nil
}

// Save - saves the events to the store if queuestore is configured,
// which will be replayed when the mongodb connection is active.
func (target *MongoDBTarget) Save(eventData event.Event) error {
	if target.store != nil {
		if target.batch != nil {
			return target.batch.Add(eventData)
		}
		_, err := target.store.Put(eventData)
		return err
	}
	if err := target.init(); err != nil {
		return err
	}
	err := target.send([]event.Event{eventData})
	if err != nil {
		if xnet.IsNetworkOrHostDown(err, false) {
			return store.ErrNotConnected
		}
	}
	return err
}

// sends a single event to the mongodb.
func (target *MongoDBTarget) send(eventData []event.Event) error {
	if len(eventData) == 0 {
		return nil
	}

	collection := target.mongoClient.Database(target.args.Database).Collection(target.args.Collection)

	_, err := collection.InsertMany(context.TODO(), eventData)
	if err != nil {
		target.loggerOnce(context.Background(), err, target.ID().String())
		return err
	}

	return nil
}

// SendFromStore - reads an event from store and adds it to mongodb
func (target *MongoDBTarget) SendFromStore(key store.Key) (err error) {
	if err = target.init(); err != nil {
		return err
	}

	var events []event.Event
	switch {
	case key.ItemCount == 1:
		var ev event.Event
		ev, err = target.store.Get(key)
		if err == nil {
			events = append(events, ev)
		}
	case key.ItemCount > 1:
		events, err = target.store.GetMultiple(key)
	}
	if err != nil {
		// The last event key in a successful batch will be sent in the channel atmost once by the replayEvents()
		// Such events will not exist and would've been already been sent successfully.
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	err = target.send(events)
	if err != nil {
		if xnet.IsNetworkOrHostDown(err, false) {
			return store.ErrNotConnected
		}
		return err
	}

	// Delete the event from store.
	return target.store.Del(key)
}

// Close - does nothing and available for interface compatibility.
func (target *MongoDBTarget) Close() error {
	if target.mongoClient != nil {
		target.mongoClient.Disconnect(context.TODO())
	}
	target.cancel()
	return nil
}

func (target *MongoDBTarget) init() error {
	return target.initOnce.Do(target.initMongoDB)
}

// Only called from init()
func (target *MongoDBTarget) initMongoDB() error {
	args := target.args
	if args.Collection == "" {
		args.Collection = defaultCollectionName
	}

	opts, err := args.toClientOptions()
	if err != nil {
		target.loggerOnce(context.Background(), err, target.ID().String())
		return err
	}

	mdb, err := mongo.Connect(opts)
	if err != nil {
		target.loggerOnce(context.Background(), err, target.ID().String())
		return err
	}

	target.mongoClient = mdb

	yes, err := target.isActive()
	if err != nil {
		target.loggerOnce(context.Background(), err, target.ID().String())
		return err
	}
	if !yes {
		target.loggerOnce(context.Background(), err, target.ID().String())
		return store.ErrNotConnected
	}

	return nil
}

// NewMongoDBTarget - creates new MongoDB target.
func NewMongoDBTarget(ctx context.Context, id string, args MongoDBArgs, loggerOnce logger.LogOnce, transport *http.Transport) (*MongoDBTarget, error) {
	var queueStore store.Store[event.Event]
	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-mongodb-"+id)
		queueStore = store.NewQueueStore[event.Event](queueDir, args.QueueLimit, event.StoreExtension)
		if err := queueStore.Open(); err != nil {
			return nil, fmt.Errorf("unable to initialize the queue store of MongoDB `%s`: %w", id, err)
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	target := &MongoDBTarget{
		id:   event.TargetID{ID: id, Name: "mongodb"},
		args: args,
		// mongoOpts:  opts,
		loggerOnce: loggerOnce,
		store:      queueStore,
		cancel:     cancel,
		cancelCh:   ctx.Done(),
	}

	if target.store != nil {
		target.batch = store.NewBatch[event.Event](store.BatchConfig[event.Event]{
			Limit:         2,
			Log:           loggerOnce,
			Store:         queueStore,
			CommitTimeout: 0, // default timeout
		})
		store.StreamItems(target.store, target, target.cancelCh, target.loggerOnce)
	}

	return target, nil
}
