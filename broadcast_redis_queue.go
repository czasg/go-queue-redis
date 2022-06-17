package queue

import (
    "context"
    "github.com/czasg/go-queue"
    "github.com/go-redis/redis"
)

func NewBroadcastRedisQueue(opt *redis.Options, rdsKey ...string) (queue.Queue, error) {
    if len(rdsKey) < 1 {
        rdsKey = append(rdsKey, "github.com/czasg/go-queue-redis")
    }
    rds := redis.NewClient(opt)
    _, err := rds.Ping().Result()
    if err != nil {
        return nil, err
    }
    return &BroadcastRedisQueue{
        rds:        rds,
        rdsChannel: rds.Subscribe(rdsKey[0]).Channel(),
        rdsKey:     rdsKey[0],
    }, nil
}

var _ queue.Queue = (*BroadcastRedisQueue)(nil)

type BroadcastRedisQueue struct {
    rds        *redis.Client
    rdsChannel <-chan *redis.Message
    rdsKey     string
}

func (r *BroadcastRedisQueue) Get(ctx context.Context) ([]byte, error) {
    if ctx == nil {
        select {
        case message := <-r.rdsChannel:
            return []byte(message.Payload), nil
        default:
            return nil, queue.ErrQueueEmpty
        }
    }
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    case message := <-r.rdsChannel:
        return []byte(message.Payload), nil
    }
}

func (r *BroadcastRedisQueue) Put(ctx context.Context, data []byte) error {
    _, err := r.rds.Publish(r.rdsKey, string(data)).Result()
    return err
}

func (r *BroadcastRedisQueue) Len() int {
    return 0
}

func (r *BroadcastRedisQueue) Close() error {
    return r.rds.Close()
}
