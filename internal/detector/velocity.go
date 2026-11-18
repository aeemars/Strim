package detector

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type VelocityTracker struct {
	Client    *redis.Client
	Window    time.Duration
	Threshold int64
}

func NewVelocityTracker(client *redis.Client) *VelocityTracker {
	return &VelocityTracker{
		Client:    client,
		Window:    60 * time.Second,
		Threshold: 5,
	}
}

var velocityScript = redis.NewScript(`
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window_start = tonumber(ARGV[2])
local transaction_id = ARGV[3]
local ttl_seconds = tonumber(ARGV[4])
redis.call("ZREMRANGEBYSCORE", key, "-inf", window_start)
redis.call("ZADD", key, now, transaction_id)
local count = redis.call("ZCARD", key)
redis.call("EXPIRE", key, ttl_seconds)
return count
`)

func (v *VelocityTracker) Count(ctx context.Context, accountID, transactionID string, eventTime time.Time) (int64, error) {
	if v.Client == nil {
		return 0, errors.New("Redis client is nil")
	}
	if accountID == "" || transactionID == "" {
		return 0, errors.New("account and transaction IDs must not be empty")
	}
	if v.Window <= 0 {
		return 0, errors.New("velocity window must be positive")
	}
	key := "account:" + accountID + ":txs"
	now := eventTime.UnixNano()
	windowStart := eventTime.Add(-v.Window).UnixNano()
	ttlSeconds := int64(v.Window/time.Second) * 2
	if ttlSeconds < 1 {
		ttlSeconds = 1
	}
	result, err := velocityScript.Run(ctx, v.Client, []string{key},
		strconv.FormatInt(now, 10),
		strconv.FormatInt(windowStart, 10),
		transactionID,
		strconv.FormatInt(ttlSeconds, 10),
	).Int64()
	if err != nil {
		return 0, fmt.Errorf("evaluate velocity: %w", err)
	}
	return result, nil
}
