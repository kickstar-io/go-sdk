package mongodb

import (
	"context"
	"errors"
	"time"

	"github.com/kickstar-io/go-sdk/mongodb/utils"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var config *Config
var client *mongo.Client
var db *mongo.Database
var dbName string

// Config struct contain extra config of mgm package.
type Config struct {
	// Set to 10 second (10*time.Second) for example.
	CtxTimeout time.Duration
}

// NewCtx function create and return new context with your specified timeout.
func NewCtx(timeout time.Duration) context.Context {
	ctx, _ := context.WithTimeout(context.Background(), timeout)

	return ctx
}

// Ctx function create new context with default
// timeout and return it.
func Ctx() context.Context {
	return ctx()
}

func ctx() context.Context {
	return NewCtx(config.CtxTimeout)
}

// NewClient return new mongodb client.
func NewClient(ctx context.Context, opts ...*options.ClientOptions) (*mongo.Client, error) {
	client, err := mongo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	if err = client.Connect(ctx); err != nil {
		return nil, err
	}

	return client, nil
}

// NewCollection return new collection with passed database
func NewCollection(db *mongo.Database, name string, opts ...*options.CollectionOptions) *Collection {
	coll := db.Collection(name, opts...)

	return &Collection{Collection: coll}
}

// ResetDefaultConfig reset all of the default config
func ResetDefaultConfig() {
	config = nil
	client = nil
	db = nil
}

//// SetDefaultConfig initial default client and Database .
//func SetDefaultConfig(conf *Config, dbName string, opts ...*options.ClientOptions) (err error) {
//
//	// Get predefined config as default config if user
//	// do not provide it.
//	if conf == nil {
//		conf = defaultConf()
//	}
//
//	config = conf
//
//	if client, err = NewClient(opts...); err != nil {
//		return err
//	}
//
//	db = client.Database(dbName)
//
//	return nil
//}

// CollectionByName return new collection from default config
func CollectionByName(name string, opts ...*options.CollectionOptions) *Collection {
	return NewCollection(db, name, opts...)
}

// CollectionByNameWithMode return new collection from default config
func CollectionByNameWithMode(name string, mode readpref.Mode) *Collection {
	if mode == readpref.SecondaryMode || mode == readpref.SecondaryPreferredMode {
		readPreference, err := readpref.New(mode)
		if err != nil {
			return NewCollection(db, name)
		} else {
			dbSecond := client.Database(dbName, &options.DatabaseOptions{
				ReadPreference: readPreference,
			})
			return NewCollection(dbSecond, name)
		}
	} else {
		return NewCollection(db, name)
	}
}

// DefaultConfigs return you'r default mongodb configs.
func DefaultConfigs() (*Config, *mongo.Client, *mongo.Database, error) {
	if utils.AnyNil(config, client, db) {
		return nil, nil, nil, errors.New("please setup default config before acquiring it")
	}

	return config, client, db, nil
}

// defaultConf is default config ,If you do not pass config
// to `SetDefaultConfig` method, we using this config.
func defaultConf() *Config {
	return &Config{CtxTimeout: 10 * time.Second}
}
