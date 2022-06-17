package queue

import (
    "context"
    "errors"
    "github.com/czasg/go-queue"
    "github.com/go-redis/redis"
    "time"
)

func NewFifoRedisQueue(opt *redis.Options, rdsKey ...string) (queue.Queue, error) {
    if len(rdsKey) < 1 {
        rdsKey = append(rdsKey, "github.com/czasg/go-queue-redis")
    }
    rds := redis.NewClient(opt)
    _, err := rds.Ping().Result()
    if err != nil {
        return nil, err
    }
    return &FifoRedisQueue{
        rds:    rds,
        rdsKey: rdsKey[0],
    }, nil
}

var _ queue.Queue = (*FifoRedisQueue)(nil)

type FifoRedisQueue struct {
    rds    *redis.Client
    rdsKey string
}

func (r *FifoRedisQueue) Get(ctx context.Context) ([]byte, error) {
    if ctx == nil {
        value, err := r.rds.LPop(r.rdsKey).Result()
        if err == nil {
            return []byte(value), nil
        }
        if errors.Is(err, redis.Nil) {
            return nil, queue.ErrQueueEmpty
        }
        return nil, err
    }
    values, err := r.rds.WithContext(ctx).BLPop(time.Hour, r.rdsKey).Result()
    if err == nil && len(values) == 2 {
        return []byte(values[1]), nil
    }
    if errors.Is(err, redis.Nil) {
        return nil, queue.ErrQueueEmpty
    }
    return nil, err
}

func (r *FifoRedisQueue) Put(ctx context.Context, data []byte) error {
    _, err := r.rds.RPush(r.rdsKey, string(data)).Result()
    return err
}

func (r *FifoRedisQueue) Len() int {
    value, _ := r.rds.LLen(r.rdsKey).Result()
    return int(value)
}

func (r *FifoRedisQueue) Close() error {
    return r.rds.Close()
}
