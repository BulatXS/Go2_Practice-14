package cache

import (
	"time"

	"github.com/redis/go-redis/v9"
)

func NewRedisClient(addr string) *redis.Client {
	if addr == "" {
		return nil
	}

	return redis.NewClient(&redis.Options{
		Addr:            addr,
		DialTimeout:     500 * time.Millisecond,
		ReadTimeout:     500 * time.Millisecond,
		WriteTimeout:    500 * time.Millisecond,
		MaxRetries:      1,
		MinRetryBackoff: 100 * time.Millisecond,
		MaxRetryBackoff: 200 * time.Millisecond,
	})
}
