package limiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/limiter"
	"github.com/aditya1808/distributed-rate-limiter/testutils"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

// Contract tests run one scenario table against the Go reference limiter and
// the Redis/Lua limiter. Scenarios use refill rates slow enough that the few
// milliseconds of real time Redis sees cannot change the outcome, so both
// implementations must produce identical results.

type step struct {
	cost          int64
	peek          bool
	wantAllowed   bool
	wantRemaining int64
}

type scenario struct {
	name       string
	rule       types.Rule
	multiplier float64
	wantLimit  int64
	steps      []step
}

func scenarios() []scenario {
	tb := func(limit int64, period time.Duration, burst int64) types.Rule {
		return types.Rule{ID: "r", Algorithm: types.AlgorithmTokenBucket, Limit: limit, Period: period, Burst: burst, Version: 1}
	}
	sw := func(limit int64, period time.Duration, burst int64, bw time.Duration) types.Rule {
		return types.Rule{ID: "r", Algorithm: types.AlgorithmSlidingWindow, Limit: limit, Period: period, Burst: burst, BurstWindow: bw, Version: 1}
	}
	return []scenario{
		{
			name: "token bucket drains burst then denies", rule: tb(1, time.Hour, 3), wantLimit: 3,
			steps: []step{{1, false, true, 2}, {1, false, true, 1}, {1, false, true, 0}, {1, false, false, 0}},
		},
		{
			name: "token bucket cost larger than remaining is denied without consuming", rule: tb(1, time.Hour, 10), wantLimit: 10,
			steps: []step{{6, false, true, 4}, {5, false, false, 4}, {4, false, true, 0}},
		},
		{
			name: "token bucket peek never consumes", rule: tb(1, time.Hour, 2), wantLimit: 2,
			steps: []step{{1, true, true, 2}, {1, true, true, 2}, {1, false, true, 1}, {1, true, true, 1}},
		},
		{
			name: "token bucket adaptive multiplier halves capacity", rule: tb(1, time.Hour, 8), multiplier: 0.5, wantLimit: 4,
			steps: []step{{1, false, true, 3}, {3, false, true, 0}, {1, false, false, 0}},
		},
		{
			name: "sliding window sustained limit", rule: sw(3, time.Hour, 0, 0), wantLimit: 3,
			steps: []step{{1, false, true, 2}, {2, false, true, 0}, {1, false, false, 0}},
		},
		{
			name: "sliding window burst cap binds before sustained", rule: sw(100, time.Hour, 2, 30*time.Minute), wantLimit: 100,
			steps: []step{{1, false, true, 1}, {1, false, true, 0}, {1, false, false, 0}},
		},
		{
			name: "sliding window peek never consumes", rule: sw(2, time.Hour, 0, 0), wantLimit: 2,
			steps: []step{{1, true, true, 2}, {1, false, true, 1}, {1, true, true, 1}},
		},
		{
			name: "sliding window adaptive multiplier", rule: sw(10, time.Hour, 0, 0), multiplier: 0.25, wantLimit: 2,
			steps: []step{{1, false, true, 1}, {1, false, true, 0}, {1, false, false, 0}},
		},
	}
}

func TestContract_MemoryAndRedisAgree(t *testing.T) {
	rdb := testutils.RedisClient(t)
	prefix := testutils.UniquePrefix(t, rdb)
	keys := constants.NewKeyspace(prefix)

	impls := map[string]func() (limiter.Limiter, func(adaptiveKey string, m float64)){
		"memory": func() (limiter.Limiter, func(string, float64)) {
			m := limiter.NewMemoryLimiter(nil)
			return m, m.SetMultiplier
		},
		"redis": func() (limiter.Limiter, func(string, float64)) {
			return limiter.NewRedisLimiter(rdb), func(key string, m float64) {
				require.NoError(t, rdb.Set(context.Background(), key, m, time.Minute).Err())
			}
		},
	}

	for implName, build := range impls {
		for i, sc := range scenarios() {
			t.Run(implName+"/"+sc.name, func(t *testing.T) {
				lim, setMult := build()
				tenant := implName + "-" + testutils.RandomSuffix()
				state := keys.State(tenant, "ns", sc.rule.ID, int64(i))
				adaptive := keys.Adaptive(tenant, "ns")
				if sc.multiplier != 0 {
					setMult(adaptive, sc.multiplier)
				}
				for n, st := range sc.steps {
					res, err := lim.Evaluate(context.Background(), limiter.Request{
						Rule: sc.rule, StateKey: state, SeqKey: keys.Seq(state), AdaptiveKey: adaptive,
						Cost: st.cost, Peek: st.peek,
					})
					require.NoError(t, err)
					assert.Equal(t, st.wantAllowed, res.Allowed, "step %d allowed", n)
					assert.Equal(t, st.wantRemaining, res.Remaining, "step %d remaining", n)
					assert.Equal(t, sc.wantLimit, res.Limit, "step %d limit", n)
					if !res.Allowed {
						assert.Positive(t, res.RetryAfter, "step %d retry_after must be set when denied", n)
					}
				}
			})
		}
	}
}

// TestRedis_TokenBucketRefillsInRealTime checks the time-dependent path that
// the contract table deliberately avoids.
func TestRedis_TokenBucketRefillsInRealTime(t *testing.T) {
	rdb := testutils.RedisClient(t)
	keys := constants.NewKeyspace(testutils.UniquePrefix(t, rdb))
	lim := limiter.NewRedisLimiter(rdb)
	rule := types.Rule{ID: "r", Algorithm: types.AlgorithmTokenBucket, Limit: 10, Period: time.Second, Burst: 2, Version: 1}
	state := keys.State("t", "ns", "r", 1)
	r := limiter.Request{Rule: rule, StateKey: state, SeqKey: keys.Seq(state), AdaptiveKey: keys.Adaptive("t", "ns"), Cost: 1}

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		res, err := lim.Evaluate(ctx, r)
		require.NoError(t, err)
		require.True(t, res.Allowed)
	}
	res, err := lim.Evaluate(ctx, r)
	require.NoError(t, err)
	assert.False(t, res.Allowed)
	assert.LessOrEqual(t, res.RetryAfter, 100*time.Millisecond)

	time.Sleep(250 * time.Millisecond) // 10/s -> at least 2 tokens back
	res, err = lim.Evaluate(ctx, r)
	require.NoError(t, err)
	assert.True(t, res.Allowed)

	ttl, err := rdb.PTTL(ctx, state).Result()
	require.NoError(t, err)
	assert.Positive(t, ttl, "state keys always carry a TTL")
}

func TestRedis_SurvivesScriptFlush(t *testing.T) {
	rdb := testutils.RedisClient(t)
	keys := constants.NewKeyspace(testutils.UniquePrefix(t, rdb))
	lim := limiter.NewRedisLimiter(rdb)
	ctx := context.Background()
	require.NoError(t, lim.Load(ctx))

	// Simulates a Redis restart or failover that drops the script cache.
	require.NoError(t, rdb.ScriptFlush(ctx).Err())

	rule := types.Rule{ID: "r", Algorithm: types.AlgorithmSlidingWindow, Limit: 5, Period: time.Minute, Version: 1}
	state := keys.State("t", "ns", "r", 1)
	res, err := lim.Evaluate(ctx, limiter.Request{Rule: rule, StateKey: state, SeqKey: keys.Seq(state), AdaptiveKey: keys.Adaptive("t", "ns"), Cost: 1})
	require.NoError(t, err, "EVALSHA falls back to EVAL on NOSCRIPT")
	assert.True(t, res.Allowed)
}
