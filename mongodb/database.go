package mongodb

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func GetDB() *mongo.Database {
	return db
}

type DBConfig struct {
	DbName     string
	UserName   string
	Password   string
	Host       string
	Port       string
	IsReplica  bool
	ReplicaSet string
}

// MongoConfig new version
type MongoConfig struct {
	DbName            string
	Host              string
	Username          string
	Password          string
	UseSRV            bool
	MaxConnectionPool uint64
}

func ConnectMongoWithConfig(dbConfig *MongoConfig, conf *Config, tlsConf *tls.Config) (context.Context, *mongo.Client, context.CancelFunc, error) {
	if conf == nil {
		conf = defaultConf()
	}
	config = conf

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	uri := ""
	if dbConfig.UseSRV {
		uri += "mongodb+srv://"
	} else {
		uri += "mongodb://"
	}

	if dbConfig.Username != "" && dbConfig.Password != "" {
		uri += dbConfig.Username + ":" + dbConfig.Password + "@"
	}

	if dbConfig.Host == "" {
		log.Fatalf("MONGODB_HOST is require")
		return ctx, nil, cancel, errors.New("MONGODB_HOST_REQUIRED")
	} else {
		uri += dbConfig.Host
	}

	// setup client
	clientOption := options.Client().ApplyURI(uri)

	// max pool size
	if dbConfig.MaxConnectionPool > 0 {
		clientOption.SetMaxPoolSize(dbConfig.MaxConnectionPool)
	}

	// tls config
	if tlsConf != nil {
		clientOption.SetTLSConfig(tlsConf)
	}

	clientNew, err := NewClient(ctx, clientOption)
	if err != nil {
		return ctx, nil, cancel, err
	}
	client = clientNew

	// ping
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatalf("[FATAL] CAN'T CONNECT TO MONGODB: %s", err.Error())
		return ctx, nil, cancel, err
	}

	// setup db
	dbName = dbConfig.DbName
	db = client.Database(dbName)

	log.Printf("[INFO] CONNECTED TO MONGO DB %s", dbName)
	return ctx, client, cancel, nil
}
