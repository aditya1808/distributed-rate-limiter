// Package testutils provides shared helpers for Redis-backed and HTTP tests.
package testutils

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisURL returns REDIS_URL or skips the test, so `go test ./...` still
// works on machines without Redis.
func RedisURL(tb testing.TB) string {
	tb.Helper()
	u := os.Getenv("REDIS_URL")
	if u == "" {
		tb.Skip("REDIS_URL not set; skipping Redis-backed test")
	}
	return u
}

// RedisClient returns a client with a pool large enough for concurrency tests.
func RedisClient(tb testing.TB) *redis.Client {
	tb.Helper()
	opts, err := redis.ParseURL(RedisURL(tb))
	if err != nil {
		tb.Fatalf("parse REDIS_URL: %v", err)
	}
	opts.PoolSize = 200
	rdb := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		tb.Fatalf("redis not reachable at REDIS_URL: %v", err)
	}
	tb.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

var unsafeChars = regexp.MustCompile(`[^a-zA-Z0-9]+`)

// UniquePrefix isolates a test's keys so packages can run in parallel
// against one Redis. Keys are deleted when the test ends.
func UniquePrefix(tb testing.TB, rdb *redis.Client) string {
	tb.Helper()
	name := strings.Trim(unsafeChars.ReplaceAllString(tb.Name(), "-"), "-")
	if len(name) > 40 {
		name = name[:40]
	}
	prefix := "test-" + name + "-" + RandomSuffix()
	tb.Cleanup(func() { DeleteKeys(rdb, prefix) })
	return prefix
}

func RandomSuffix() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func DeleteKeys(rdb *redis.Client, prefix string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	iter := rdb.Scan(ctx, 0, prefix+":*", 500).Iterator()
	var batch []string
	for iter.Next(ctx) {
		batch = append(batch, iter.Val())
		if len(batch) == 500 {
			rdb.Del(ctx, batch...)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		rdb.Del(ctx, batch...)
	}
}
