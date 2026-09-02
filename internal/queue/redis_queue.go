package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	streamKey  = "djs:jobs"
	groupName  = "djs:workers"
)

type RedisQueue struct {
	client *redis.Client
}

func NewRedisQueue(client *redis.Client) *RedisQueue {
	return &RedisQueue{client: client}
}

// EnsureGroup creates the consumer group if it doesn't exist yet. Call this
// once at worker startup before Consume.
func (q *RedisQueue) EnsureGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, streamKey, groupName, "0").Err()
	if err != nil && !isBusyGroupErr(err) {
		return fmt.Errorf("ensure consumer group: %w", err)
	}
	return nil
}

func (q *RedisQueue) Publish(ctx context.Context, jobID string) error {
	err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{"job_id": jobID},
	}).Err()
	if err != nil {
		return fmt.Errorf("publish job %s: %w", jobID, err)
	}
	return nil
}

func (q *RedisQueue) Consume(ctx context.Context, consumerName string) (<-chan Message, error) {
	if err := q.EnsureGroup(ctx); err != nil {
		return nil, err
	}

	out := make(chan Message)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumerName,
				Streams:  []string{streamKey, ">"},
				Count:    5,
				Block:    2 * time.Second,
			}).Result()
			if err != nil {
				if err == redis.Nil || ctx.Err() != nil {
					continue
				}
				time.Sleep(time.Second) // avoid hot-looping on persistent errors
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					jobID, _ := msg.Values["job_id"].(string)
					id := msg.ID
					out <- Message{
						JobID: jobID,
						Ack: func(ctx context.Context) error {
							return q.client.XAck(ctx, streamKey, groupName, id).Err()
						},
					}
				}
			}
		}
	}()
	return out, nil
}

func isBusyGroupErr(err error) bool {
	return err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists"
}
