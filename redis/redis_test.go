package searedis

import (
	"context"
	"testing"
)

func TestPushList(t *testing.T) {
	ConnectRedisV1(&RedisConnectionConfig{
		Addr:     "aaa174573fcfd43d59bcbc58416e9c44-78f0dc04d017e194.elb.ap-southeast-1.amazonaws.com:6379",
		UserName: "",
		Password: "kYRvzpvIze9jyE",
		Database: 12,
		PoolSize: 100,
	})
	LPushList(context.Background(), "nft_token_ids", 1)
}
func TestLPopList(t *testing.T) {
	ConnectRedisV1(&RedisConnectionConfig{
		Addr:     "aaa174573fcfd43d59bcbc58416e9c44-78f0dc04d017e194.elb.ap-southeast-1.amazonaws.com:6379",
		UserName: "",
		Password: "kYRvzpvIze9jyE",
		Database: 12,
		PoolSize: 100,
	})
	LPopList(context.Background(), "redeem_event")
}
func TestBulkPushList(t *testing.T) {
	ConnectRedisV1(&RedisConnectionConfig{
		Addr:     "aaa174573fcfd43d59bcbc58416e9c44-78f0dc04d017e194.elb.ap-southeast-1.amazonaws.com:6379",
		UserName: "",
		Password: "kYRvzpvIze9jyE",
		Database: 12,
		PoolSize: 100,
	})
	for i := 1005; i < 1015; i++ {
		LPushList(context.Background(), "nft_token_ids_test", i)
	}
}

func TestShuffleList(t *testing.T) {
	ConnectRedisV1(&RedisConnectionConfig{
		Addr:     "aaa174573fcfd43d59bcbc58416e9c44-78f0dc04d017e194.elb.ap-southeast-1.amazonaws.com:6379",
		UserName: "",
		Password: "kYRvzpvIze9jyE",
		Database: 12,
		PoolSize: 100,
	})
	ShuffeList(context.Background(), "nft_token_ids_test")
}
