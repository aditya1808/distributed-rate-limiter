package limiter_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/limiter"
	"github.com/aditya1808/distributed-rate-limiter/testutils"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

const (
	goroutines = 500
	burst      = 50
)

// The refill rate is 1 per hour, so during the test the only quota that can
// ever exist is the burst: exactly `burst` requests must be allowed.
func contentionRules() map[string]types.Rule {
	return map[string]types.Rule{
		"token_bucket":   {ID: "r", Algorithm: types.AlgorithmTokenBucket, Limit: 1, Period: time.Hour, Burst: burst, Version: 1},
		"sliding_window": {ID: "r", Algorithm: types.AlgorithmSlidingWindow, Limit: burst, Period: time.Hour, Version: 1},
	}
}

// hammer releases all goroutines at once against the same key and counts
// how many were allowed.
func hammer(t *testing.T, lim limiter.Limiter, req limiter.Request) int64 {
	t.Helper()
	var (
		allowed atomic.Int64
		wg      sync.WaitGroup
		start   = make(chan struct{})
	)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			res, err := lim.Evaluate(ctx, req)
			if err != nil {
				t.Errorf("evaluate: %v", err)
				return
			}
			if res.Allowed {
				allowed.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	return allowed.Load()
}

func TestConcurrentSameKey_Redis(t *testing.T) {
	rdb := testutils.RedisClient(t)
	keys := constants.NewKeyspace(testutils.UniquePrefix(t, rdb))
	lim := limiter.NewRedisLimiter(rdb)
	require.NoError(t, lim.Load(context.Background()))

	for name, rule := range contentionRules() {
		t.Run(name, func(t *testing.T) {
			state := keys.State("tenant-"+name, "ns", rule.ID, 1)
			allowed := hammer(t, lim, limiter.Request{
				Rule: rule, StateKey: state, SeqKey: keys.Seq(state),
				AdaptiveKey: keys.Adaptive("tenant-"+name, "ns"), Cost: 1,
			})
			assert.EqualValues(t, burst, allowed, "atomic Lua evaluation must admit exactly the burst")
		})
	}
}

func TestConcurrentSameKey_Memory(t *testing.T) {
	for name, rule := range contentionRules() {
		t.Run(name, func(t *testing.T) {
			allowed := hammer(t, limiter.NewMemoryLimiter(nil), limiter.Request{Rule: rule, StateKey: "k", Cost: 1})
			assert.EqualValues(t, burst, allowed)
		})
	}
}

// naiveLimiter is the read-modify-write bug the Lua scripts exist to prevent:
// GET, decide in Go, then SET. The sleep stands in for network latency
// between the two round trips.
type naiveLimiter struct{ rdb *redis.Client }

func (n naiveLimiter) Evaluate(ctx context.Context, req limiter.Request) (limiter.Result, error) {
	remaining, err := n.rdb.Get(ctx, req.StateKey).Int64()
	if errors.Is(err, redis.Nil) {
		remaining = req.Rule.Capacity()
	} else if err != nil {
		return limiter.Result{}, err
	}
	if remaining < req.Cost {
		return limiter.Result{Allowed: false}, nil
	}
	time.Sleep(2 * time.Millisecond)
	if err := n.rdb.Set(ctx, req.StateKey, remaining-req.Cost, time.Minute).Err(); err != nil {
		return limiter.Result{}, err
	}
	return limiter.Result{Allowed: true}, nil
}

// TestConcurrentSameKey_NaiveControl proves the concurrency test can detect
// the race: the same workload against a non-atomic limiter over-admits.
func TestConcurrentSameKey_NaiveControl(t *testing.T) {
	rdb := testutils.RedisClient(t)
	keys := constants.NewKeyspace(testutils.UniquePrefix(t, rdb))
	rule := contentionRules()["token_bucket"]

	allowed := hammer(t, naiveLimiter{rdb: rdb}, limiter.Request{Rule: rule, StateKey: keys.State("naive", "ns", "r", 1), Cost: 1})
	assert.Greater(t, allowed, int64(burst), "a non-atomic GET/SET limiter must over-admit under contention")
	t.Logf("naive limiter admitted %d requests against a limit of %d", allowed, burst)
}
