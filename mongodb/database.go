package mongodb

import (
	"context"
	"crypto/tls"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
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
	ConnectionString  string
	MaxConnectionPool uint64
}

func defaultDB() *DBConfig {
	dbCfg := &DBConfig{}
	return dbCfg
}

func ConnectMongoWithConfig(dbConfig *MongoConfig, conf *Config) (context.Context, *mongo.Client, context.CancelFunc, error) {
	if conf == nil {
		conf = defaultConf()
	}

	config = conf
	dbName = dbConfig.DbName
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	clientOption := options.Client().ApplyURI(dbConfig.ConnectionString)

	if dbConfig.MaxConnectionPool > 0 {
		clientOption.SetMaxPoolSize(dbConfig.MaxConnectionPool)
	}

	// disable tls
	clientOption.SetTLSConfig(&tls.Config{
		InsecureSkipVerify: true,
	})

	clientNew, err := NewClient(ctx, clientOption)
	if err != nil {
		return ctx, nil, cancel, err
	}
	client = clientNew

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatalf("[FATAL] CAN'T CONNECTING TO MONGODB: %s", err.Error())
		return ctx, nil, cancel, err
	}

	db = client.Database(dbName)

	log.Printf("[INFO] CONNECTED TO MONGO DB %s", dbName)
	return ctx, client, cancel, nil
}
