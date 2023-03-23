package searedis

import (
	"context"
	"github.com/go-redis/redis/v8"
	goredislib "github.com/go-redis/redis/v8"
)

type TransactionFunc func(pipe redis.Pipeliner) error

func TransactionWithCtx(ctx context.Context, f TransactionFunc) error {
	return TransactionWithClient(ctx, client, f)
}

func TransactionWithClient(ctx context.Context, client *goredislib.Client, f TransactionFunc) error {
	err := client.Watch(ctx, func(tx *redis.Tx) error {

		_, err := tx.TxPipelined(ctx, f)

		return err
	})

	return err
}
