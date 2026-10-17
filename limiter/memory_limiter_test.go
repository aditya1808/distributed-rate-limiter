package limiter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/types"
)

type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock               { return &fakeClock{t: time.UnixMilli(1_700_000_000_000)} }
func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }
func req(rule types.Rule, cost int64) Request {
	return Request{Rule: rule, StateKey: "k", AdaptiveKey: "a", Cost: cost}
}

func tokenBucket(limit int64, period time.Duration, burst int64) types.Rule {
	return types.Rule{ID: "r", Algorithm: types.AlgorithmTokenBucket, Limit: limit, Period: period, Burst: burst, Version: 1}
}

func slidingWindow(limit int64, period time.Duration, burst int64, burstWindow time.Duration) types.Rule {
	return types.Rule{ID: "r", Algorithm: types.AlgorithmSlidingWindow, Limit: limit, Period: period,
		Burst: burst, BurstWindow: burstWindow, Version: 1}
}

func mustEval(t *testing.T, l *MemoryLimiter, r Request) Result {
	t.Helper()
	res, err := l.Evaluate(context.Background(), r)
	require.NoError(t, err)
	return res
}

func TestTokenBucket_BurstThenDeny(t *testing.T) {
	clock := newFakeClock()
	l := NewMemoryLimiter(clock.Now)
	rule := tokenBucket(300, time.Minute, 20) // 5/s sustained, burst 20

	for i := 0; i < 20; i++ {
		res := mustEval(t, l, req(rule, 1))
		require.True(t, res.Allowed, "request %d", i)
		assert.EqualValues(t, 19-i, res.Remaining)
	}
	res := mustEval(t, l, req(rule, 1))
	assert.False(t, res.Allowed)
	assert.EqualValues(t, 0, res.Remaining)
	assert.Equal(t, 200*time.Millisecond, res.RetryAfter, "one token refills every 200ms at 5/s")
	assert.Equal(t, 4*time.Second, res.ResetAfter, "20 tokens at 5/s")
	assert.EqualValues(t, 20, res.Limit)
}

func TestTokenBucket_RefillIsExact(t *testing.T) {
	clock := newFakeClock()
	l := NewMemoryLimiter(clock.Now)
	rule := tokenBucket(300, time.Minute, 20)
	for i := 0; i < 20; i++ {
		mustEval(t, l, req(rule, 1))
	}

	clock.Advance(199 * time.Millisecond)
	assert.False(t, mustEval(t, l, req(rule, 1)).Allowed, "less than one token after 199ms")

	clock.Advance(1 * time.Millisecond)
	res := mustEval(t, l, req(rule, 1))
	assert.True(t, res.Allowed, "exactly one token after 200ms")
	assert.EqualValues(t, 0, res.Remaining)

	clock.Advance(time.Hour)
	assert.EqualValues(t, 19, mustEval(t, l, req(rule, 1)).Remaining, "refill is capped at capacity")
}

func TestTokenBucket_CostAndDefaultBurst(t *testing.T) {
	clock := newFakeClock()
	l := NewMemoryLimiter(clock.Now)
	rule := tokenBucket(10, time.Second, 0) // burst defaults to limit

	res := mustEval(t, l, req(rule, 7))
	assert.True(t, res.Allowed)
	assert.EqualValues(t, 3, res.Remaining)
	assert.EqualValues(t, 10, res.Limit)

	res = mustEval(t, l, req(rule, 4))
	assert.False(t, res.Allowed, "denied request consumes nothing")
	assert.EqualValues(t, 3, res.Remaining)
	assert.Equal(t, 100*time.Millisecond, res.RetryAfter, "needs 1 more token at 10/s")
}

func TestTokenBucket_PeekDoesNotConsume(t *testing.T) {
	l := NewMemoryLimiter(newFakeClock().Now)
	rule := tokenBucket(10, time.Second, 5)
	peek := req(rule, 1)
	peek.Peek = true
	for i := 0; i < 10; i++ {
		res := mustEval(t, l, peek)
		assert.True(t, res.Allowed)
		assert.EqualValues(t, 5, res.Remaining)
		assert.EqualValues(t, 0, res.Used)
	}
}

func TestTokenBucket_AdaptiveMultiplierShrinksCapacity(t *testing.T) {
	l := NewMemoryLimiter(newFakeClock().Now)
	rule := tokenBucket(100, time.Second, 20)
	l.SetMultiplier("a", 0.5)

	allowed := 0
	for i := 0; i < 20; i++ {
		res := mustEval(t, l, req(rule, 1))
		if res.Allowed {
			allowed++
		}
		assert.EqualValues(t, 10, res.Limit)
		assert.Equal(t, 0.5, res.Multiplier)
	}
	assert.Equal(t, 10, allowed)
}

func TestSlidingWindow_SustainedLimit(t *testing.T) {
	clock := newFakeClock()
	l := NewMemoryLimiter(clock.Now)
	rule := slidingWindow(5, 10*time.Second, 0, 0)

	for i := 0; i < 5; i++ {
		require.True(t, mustEval(t, l, req(rule, 1)).Allowed)
		clock.Advance(time.Second)
	}
	// t=5s: window holds requests at 0..4s
	res := mustEval(t, l, req(rule, 1))
	assert.False(t, res.Allowed)
	assert.Equal(t, 5*time.Second, res.RetryAfter, "oldest entry (t=0) leaves the window at t=10s")

	clock.Advance(5 * time.Second) // t=10s: t=0 entry has expired
	res = mustEval(t, l, req(rule, 1))
	assert.True(t, res.Allowed)
	assert.EqualValues(t, 0, res.Remaining)
}

func TestSlidingWindow_BurstCapIndependentOfSustained(t *testing.T) {
	clock := newFakeClock()
	l := NewMemoryLimiter(clock.Now)
	rule := slidingWindow(100, time.Minute, 3, time.Second) // 100/min, max 3 per second

	for i := 0; i < 3; i++ {
		require.True(t, mustEval(t, l, req(rule, 1)).Allowed)
	}
	res := mustEval(t, l, req(rule, 1))
	assert.False(t, res.Allowed, "burst window is full even though sustained has room")
	assert.Equal(t, time.Second, res.RetryAfter)
	assert.EqualValues(t, 0, res.Remaining)

	clock.Advance(time.Second)
	res = mustEval(t, l, req(rule, 1))
	assert.True(t, res.Allowed)
	assert.EqualValues(t, 2, res.Remaining, "min(100-4, 3-1)")
	assert.EqualValues(t, 4, res.Used)
}

func TestSlidingWindow_CostAndReset(t *testing.T) {
	clock := newFakeClock()
	l := NewMemoryLimiter(clock.Now)
	rule := slidingWindow(10, time.Minute, 0, 0)

	res := mustEval(t, l, req(rule, 6))
	assert.True(t, res.Allowed)
	assert.EqualValues(t, 4, res.Remaining)
	assert.Equal(t, time.Minute, res.ResetAfter)

	clock.Advance(10 * time.Second)
	res = mustEval(t, l, req(rule, 5))
	assert.False(t, res.Allowed)
	assert.Equal(t, 50*time.Second, res.RetryAfter, "the cost-6 batch from t=0 must expire")
	assert.EqualValues(t, 4, res.Remaining)
}

func TestSlidingWindow_PeekDoesNotConsumeOrTrim(t *testing.T) {
	clock := newFakeClock()
	l := NewMemoryLimiter(clock.Now)
	rule := slidingWindow(3, time.Second, 0, 0)
	mustEval(t, l, req(rule, 2))

	peek := req(rule, 1)
	peek.Peek = true
	res := mustEval(t, l, peek)
	assert.EqualValues(t, 2, res.Used)
	assert.EqualValues(t, 1, res.Remaining)

	clock.Advance(2 * time.Second)
	res = mustEval(t, l, peek)
	assert.EqualValues(t, 0, res.Used, "expired entries are not counted")
	assert.EqualValues(t, 3, res.Remaining)
}
