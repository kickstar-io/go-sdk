package searedis

import (
	"context"
	"encoding/json"
	"time"

	plock "github.com/kickstar-io/go-sdk/redis/lock"

	goredislib "github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4/redis"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
)

var client *goredislib.Client

func NewPool() redis.Pool {
	return goredis.NewPool(client)
}

func ConnectRedisV1(config *RedisConnectionConfig) error {
	client = goredislib.NewClient(&goredislib.Options{
		Addr:         config.Addr,
		Username:     config.UserName,
		Password:     config.Password,
		DB:           config.Database,
		PoolSize:     config.PoolSize,
		MaxRetries:   2,
		IdleTimeout:  5 * time.Minute,
		MinIdleConns: 10,
		WriteTimeout: time.Duration(600) * time.Second,
		ReadTimeout:  time.Duration(600) * time.Second,
		DialTimeout:  time.Duration(600) * time.Second,
	})
	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		return err
	}
	plock.InitPool(NewPool())

	return nil
}

func GetClient() *goredislib.Client {
	return client
}

// SetObject Set struct to redis
// EX used: err = util.SetObject(ctx, "key", userModel, 86400)
func SetObject(ctx context.Context, key string, value interface{}, expirationSecond int) error {
	jsonStr, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return client.Set(ctx, key, jsonStr, time.Duration(expirationSecond)*time.Second).Err()
}

// SetObjectWithPipe Set struct to redis
// EX used: err = util.SetObject(ctx, "key", userModel, 86400)
func SetObjectWithPipe(ctx context.Context, pipe goredislib.Pipeliner, key string, value interface{}, expirationSecond int) error {
	jsonStr, err := json.Marshal(value)
	if err != nil {
		return err
	}

	_, err = pipe.Set(ctx, key, jsonStr, time.Duration(expirationSecond)*time.Second).Result()
	return err
}

func IncrObject(ctx context.Context, key string) error {
	_, err := client.Incr(ctx, key).Result()
	if err != nil {
		return err
	}
	return nil
}

// GetObject Get struct from redis
// EX used: existsFlag, err := util.GetObject(ctx, "key", &userModel)
func GetObject(ctx context.Context, key string, refObj interface{}) (bool, error) {
	bytes, err := client.Get(ctx, key).Bytes()

	if err != nil {
		if err == goredislib.Nil {
			// Key not exists
			return false, nil
		}

		return false, err
	}

	err = json.Unmarshal(bytes, &refObj)
	if err != nil {
		return false, err
	}
	return true, nil
}

// GetObjectWithPipe Get struct from redis
// EX used: existsFlag, err := util.GetObject(ctx, "key", &userModel)
func GetObjectWithPipe(ctx context.Context, pipe goredislib.Pipeliner, key string, refObj interface{}) (bool, error) {
	bytes, err := pipe.Get(ctx, key).Bytes()

	if err != nil {
		if err == goredislib.Nil {
			// Key not exists
			return false, nil
		}

		return false, err
	}

	err = json.Unmarshal(bytes, &refObj)
	if err != nil {
		return false, err
	}
	return true, nil
}

// SetList Set list to redis
func LPushList(ctx context.Context, key string, value interface{}) error {
	jsonStr, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return client.LPush(ctx, key, jsonStr).Err()
}

// LPopList : get and remove left item of the list
func LPopList(ctx context.Context, key string) (interface{}, error) {
	cmd := client.LPop(ctx, key)
	return cmd.Val(), cmd.Err()
}
