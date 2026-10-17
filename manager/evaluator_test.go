package manager

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/limiter"
	"github.com/aditya1808/distributed-rate-limiter/metrics"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

type fakeResolver map[string]types.Rule

func (f fakeResolver) Resolve(tenant, ns string) (types.Rule, types.MatchKind, bool) {
	if r, ok := f[constants.PairKey(tenant, ns)]; ok {
		return r, types.MatchTenant, true
	}
	if r, ok := f[constants.PairKey("*", ns)]; ok {
		return r, types.MatchDefault, true
	}
	return types.Rule{}, "", false
}

type fakeLimiter struct {
	calls []limiter.Request
	res   limiter.Result
	err   error
}

func (f *fakeLimiter) Evaluate(ctx context.Context, req limiter.Request) (limiter.Result, error) {
	f.calls = append(f.calls, req)
	return f.res, f.err
}

type nopActivity struct{ touched int }

func (n *nopActivity) Touch(string, string) { n.touched++ }

func newEvaluator(rules fakeResolver, lim limiter.Limiter, defaultAllow bool) (*Evaluator, *nopActivity) {
	act := &nopActivity{}
	return NewEvaluator(rules, lim, act, metrics.New(), constants.NewKeyspace("rl"),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		EvaluatorConfig{DefaultAllow: defaultAllow, StoreTimeout: 50 * time.Millisecond}), act
}

var defaultRule = types.Rule{ID: "r1", Tenant: "*", Namespace: "orders", Algorithm: types.AlgorithmTokenBucket,
	Limit: 10, Period: time.Second, Burst: 5, Version: 3}

func TestEvaluate_DefaultRuleGivesEachTenantItsOwnKey(t *testing.T) {
	lim := &fakeLimiter{res: limiter.Result{Allowed: true, Limit: 5, Remaining: 4, Multiplier: 1}}
	e, act := newEvaluator(fakeResolver{"*|orders": defaultRule}, lim, true)

	_, err := e.Evaluate(context.Background(), "acme", "orders", 1)
	require.NoError(t, err)
	_, err = e.Evaluate(context.Background(), "globex", "orders", 1)
	require.NoError(t, err)

	require.Len(t, lim.calls, 2)
	assert.Equal(t, "rl:{acme|orders}:r1:v3", lim.calls[0].StateKey)
	assert.Equal(t, "rl:{globex|orders}:r1:v3", lim.calls[1].StateKey)
	assert.Equal(t, "rl:{acme|orders}:r1:v3:seq", lim.calls[0].SeqKey)
	assert.Equal(t, "rl:{acme|orders}:adaptive", lim.calls[0].AdaptiveKey)
	assert.Equal(t, 2, act.touched)
}

func TestEvaluate_NoRuleFollowsDefaultPolicy(t *testing.T) {
	for _, allow := range []bool{true, false} {
		lim := &fakeLimiter{}
		e, _ := newEvaluator(fakeResolver{}, lim, allow)
		d, err := e.Evaluate(context.Background(), "acme", "orders", 1)
		require.NoError(t, err)
		assert.Equal(t, allow, d.Allowed)
		assert.Equal(t, types.ReasonNoRule, d.Reason)
		assert.Empty(t, lim.calls, "no store call without a rule")
	}
}

func TestEvaluate_CostAboveCapacityDeniedWithoutStoreCall(t *testing.T) {
	lim := &fakeLimiter{}
	e, _ := newEvaluator(fakeResolver{"*|orders": defaultRule}, lim, true)
	d, err := e.Evaluate(context.Background(), "acme", "orders", 6)
	require.NoError(t, err)
	assert.False(t, d.Allowed)
	assert.Equal(t, types.ReasonCostExceedsCapacity, d.Reason)
	assert.Empty(t, lim.calls)
}

func TestEvaluate_StoreFailureAppliesFailMode(t *testing.T) {
	for _, failOpen := range []bool{true, false} {
		r := defaultRule
		r.FailOpen = failOpen
		lim := &fakeLimiter{err: errors.New("connection refused")}
		e, _ := newEvaluator(fakeResolver{"*|orders": r}, lim, true)

		d, err := e.Evaluate(context.Background(), "acme", "orders", 1)
		require.NoError(t, err, "a store outage is a degraded decision, not an error")
		assert.Equal(t, failOpen, d.Allowed)
		assert.True(t, d.Degraded)
		assert.Equal(t, types.ReasonStoreUnavailable, d.Reason)
	}
}

func TestEvaluate_CallerCancellationIsAnError(t *testing.T) {
	lim := &fakeLimiter{err: context.Canceled}
	e, _ := newEvaluator(fakeResolver{"*|orders": defaultRule}, lim, true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := e.Evaluate(ctx, "acme", "orders", 1)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestEvaluate_MapsResult(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000).UTC()
	lim := &fakeLimiter{res: limiter.Result{Allowed: false, Limit: 5, Remaining: 0, RetryAfter: 200 * time.Millisecond,
		ResetAfter: time.Second, Now: now, Multiplier: 0.5}}
	e, _ := newEvaluator(fakeResolver{"*|orders": defaultRule}, lim, true)

	d, err := e.Evaluate(context.Background(), "acme", "orders", 1)
	require.NoError(t, err)
	assert.False(t, d.Allowed)
	assert.Equal(t, types.ReasonQuotaExceeded, d.Reason)
	assert.Equal(t, now.Add(time.Second), d.ResetAt)
	assert.Equal(t, 200*time.Millisecond, d.RetryAfter)
	assert.Equal(t, 0.5, d.Multiplier)
	assert.Equal(t, "r1", d.RuleID)
}

func TestPeek(t *testing.T) {
	lim := &fakeLimiter{res: limiter.Result{Allowed: true, Limit: 5, Used: 2, Remaining: 3, Multiplier: 1}}
	e, act := newEvaluator(fakeResolver{"*|orders": defaultRule}, lim, true)

	q, err := e.Peek(context.Background(), "acme", "orders")
	require.NoError(t, err)
	assert.EqualValues(t, 2, q.Used)
	assert.Equal(t, types.MatchDefault, q.Match)
	assert.True(t, lim.calls[0].Peek)
	assert.Zero(t, act.touched, "peeking is not activity")

	_, err = e.Peek(context.Background(), "acme", "payments")
	assert.ErrorIs(t, err, types.ErrNoRule)

	lim.err = errors.New("down")
	_, err = e.Peek(context.Background(), "acme", "orders")
	assert.ErrorIs(t, err, types.ErrStoreUnavailable)
}
