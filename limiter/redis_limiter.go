package limiter

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aditya1808/distributed-rate-limiter/types"
)

var (
	//go:embed scripts/token_bucket.lua
	tokenBucketSrc string
	//go:embed scripts/sliding_window.lua
	slidingWindowSrc string
)

// RedisLimiter evaluates quotas with Lua scripts so that read, decide and
// write happen as one atomic step, regardless of how many instances call it.
type RedisLimiter struct {
	rdb           redis.Scripter
	tokenBucket   *redis.Script
	slidingWindow *redis.Script
}

func NewRedisLimiter(rdb redis.Scripter) *RedisLimiter {
	return &RedisLimiter{
		rdb:           rdb,
		tokenBucket:   redis.NewScript(tokenBucketSrc),
		slidingWindow: redis.NewScript(slidingWindowSrc),
	}
}

// Load pre-registers the scripts so the first request uses EVALSHA. Run also
// falls back to EVAL on NOSCRIPT, e.g. after a Redis restart or failover.
func (l *RedisLimiter) Load(ctx context.Context) error {
	if err := l.tokenBucket.Load(ctx, l.rdb).Err(); err != nil {
		return fmt.Errorf("load token bucket script: %w", err)
	}
	if err := l.slidingWindow.Load(ctx, l.rdb).Err(); err != nil {
		return fmt.Errorf("load sliding window script: %w", err)
	}
	return nil
}

func (l *RedisLimiter) Evaluate(ctx context.Context, req Request) (Result, error) {
	var (
		raw any
		err error
	)
	switch req.Rule.Algorithm {
	case types.AlgorithmTokenBucket:
		raw, err = l.tokenBucket.Run(ctx, l.rdb,
			[]string{req.StateKey, req.AdaptiveKey},
			req.Rule.Capacity(),
			strconv.FormatFloat(refillPerMs(req.Rule), 'f', -1, 64),
			req.Cost,
			peekArg(req.Peek),
		).Result()
	case types.AlgorithmSlidingWindow:
		raw, err = l.slidingWindow.Run(ctx, l.rdb,
			[]string{req.StateKey, req.AdaptiveKey, req.SeqKey},
			req.Rule.Limit,
			req.Rule.Period.Milliseconds(),
			req.Rule.Burst,
			req.Rule.BurstWindow.Milliseconds(),
			req.Cost,
			peekArg(req.Peek),
		).Result()
	default:
		return Result{}, fmt.Errorf("unknown algorithm %q", req.Rule.Algorithm)
	}
	if err != nil {
		return Result{}, fmt.Errorf("evaluate %s: %w", req.Rule.Algorithm, err)
	}
	return parseResult(raw)
}

func peekArg(peek bool) string {
	if peek {
		return "1"
	}
	return "0"
}

func parseResult(raw any) (Result, error) {
	vals, ok := raw.([]any)
	if !ok || len(vals) != 8 {
		return Result{}, fmt.Errorf("unexpected script result %#v", raw)
	}
	ints := make([]int64, 8)
	for _, i := range []int{0, 1, 2, 3, 4, 5, 7} {
		n, ok := vals[i].(int64)
		if !ok {
			return Result{}, fmt.Errorf("unexpected script result element %d: %#v", i, vals[i])
		}
		ints[i] = n
	}
	multStr, ok := vals[6].(string)
	if !ok {
		return Result{}, fmt.Errorf("unexpected multiplier %#v", vals[6])
	}
	mult, err := strconv.ParseFloat(multStr, 64)
	if err != nil {
		return Result{}, fmt.Errorf("parse multiplier %q: %w", multStr, err)
	}
	return Result{
		Allowed:    ints[0] == 1,
		Remaining:  ints[1],
		Limit:      ints[2],
		RetryAfter: time.Duration(ints[3]) * time.Millisecond,
		ResetAfter: time.Duration(ints[4]) * time.Millisecond,
		Now:        time.UnixMilli(ints[5]).UTC(),
		Multiplier: mult,
		Used:       ints[7],
	}, nil
}
