package cache

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/metrics"
	"github.com/aditya1808/distributed-rate-limiter/persistence/persistencetest"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

func rule(id string, version int64) types.Rule {
	return types.Rule{ID: id, Algorithm: types.AlgorithmTokenBucket, Limit: 10, Period: time.Second, Version: version}
}

type fakeLookup struct {
	calls atomic.Int64
	delay time.Duration
	res   types.Resolution
	err   error
}

func (f *fakeLookup) Lookup(context.Context, string, string) (types.Resolution, string, error) {
	f.calls.Add(1)
	time.Sleep(f.delay)
	return f.res, "postgres", f.err
}

func startCache(t *testing.T, store *persistencetest.MemoryBindings, fallback Lookup, refresh time.Duration) *RuleCache {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var lookup Lookup
	if fallback != nil {
		lookup = fallback
	}
	c := New(store, store, lookup, Options{Refresh: refresh, MissTTL: time.Minute}, metrics.New(),
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, c.Start(ctx))
	t.Cleanup(func() {
		cancel()
		<-c.Done()
	})
	return c
}

func TestResolve_TenantAssignmentBeatsDefault(t *testing.T) {
	store := persistencetest.NewMemoryBindings()
	store.AssignSilently("*", "orders", rule("default", 1))
	store.AssignSilently("acme", "orders", rule("acme", 1))
	c := startCache(t, store, nil, time.Hour)
	ctx := context.Background()

	r, match, ok := c.Resolve(ctx, "acme", "orders")
	require.True(t, ok)
	assert.Equal(t, "acme", r.ID)
	assert.Equal(t, types.MatchTenant, match)

	r, match, ok = c.Resolve(ctx, "globex", "orders")
	require.True(t, ok)
	assert.Equal(t, "default", r.ID)
	assert.Equal(t, types.MatchDefault, match)

	_, _, ok = c.Resolve(ctx, "acme", "payments")
	assert.False(t, ok)
	assert.True(t, c.Loaded())
}

func TestResolve_OneRuleServesManyPairs(t *testing.T) {
	store := persistencetest.NewMemoryBindings()
	shared := rule("pro", 1)
	store.AssignSilently("acme", "orders", shared)
	store.AssignSilently("globex", "search", shared)
	c := startCache(t, store, nil, time.Hour)

	a, _, _ := c.Resolve(context.Background(), "acme", "orders")
	b, _, _ := c.Resolve(context.Background(), "globex", "search")
	assert.Equal(t, "pro", a.ID)
	assert.Equal(t, "pro", b.ID)
	assert.Equal(t, 2, c.Len())
}

func TestChangeEventsTriggerReload(t *testing.T) {
	store := persistencetest.NewMemoryBindings()
	c := startCache(t, store, nil, time.Hour)
	ctx := context.Background()

	store.Assign("acme", "orders", rule("r1", 1))
	require.Eventually(t, func() bool { _, _, ok := c.Resolve(ctx, "acme", "orders"); return ok }, time.Second, 5*time.Millisecond)

	updated := rule("r1", 2)
	updated.Limit = 99
	store.Assign("acme", "orders", updated)
	require.Eventually(t, func() bool { r, _, _ := c.Resolve(ctx, "acme", "orders"); return r.Limit == 99 }, time.Second, 5*time.Millisecond)

	store.Unassign("acme", "orders")
	require.Eventually(t, func() bool { _, _, ok := c.Resolve(ctx, "acme", "orders"); return !ok }, time.Second, 5*time.Millisecond)
}

func TestPeriodicReloadRecoversLostEvents(t *testing.T) {
	store := persistencetest.NewMemoryBindings()
	c := startCache(t, store, nil, 50*time.Millisecond)

	store.AssignSilently("acme", "orders", rule("r1", 1))
	require.Eventually(t, func() bool { _, _, ok := c.Resolve(context.Background(), "acme", "orders"); return ok },
		2*time.Second, 10*time.Millisecond)
}

func TestMiss_FallsBackAndReusesTheAnswer(t *testing.T) {
	lookup := &fakeLookup{res: types.Resolution{Found: true, Match: types.MatchTenant, Rule: rule("late", 1)}}
	c := startCache(t, persistencetest.NewMemoryBindings(), lookup, time.Hour)

	for range 5 {
		r, match, ok := c.Resolve(context.Background(), "acme", "orders")
		require.True(t, ok, "a rule missing from the snapshot is found through the fallback")
		assert.Equal(t, "late", r.ID)
		assert.Equal(t, types.MatchTenant, match)
	}
	assert.EqualValues(t, 1, lookup.calls.Load())
}

func TestMiss_NoRuleIsCachedToo(t *testing.T) {
	lookup := &fakeLookup{}
	c := startCache(t, persistencetest.NewMemoryBindings(), lookup, time.Hour)

	for range 100 {
		_, _, ok := c.Resolve(context.Background(), "acme", "orders")
		assert.False(t, ok)
	}
	assert.EqualValues(t, 1, lookup.calls.Load(), "unmatched traffic must not query the store on every request")
}

func TestMiss_ConcurrentCallersShareOneLookup(t *testing.T) {
	lookup := &fakeLookup{delay: 20 * time.Millisecond}
	c := startCache(t, persistencetest.NewMemoryBindings(), lookup, time.Hour)

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() { c.Resolve(context.Background(), "acme", "orders") })
	}
	wg.Wait()
	assert.EqualValues(t, 1, lookup.calls.Load())
}

func TestMiss_FailureIsTreatedAsNoRuleAndCached(t *testing.T) {
	lookup := &fakeLookup{err: errors.New("postgres down")}
	c := startCache(t, persistencetest.NewMemoryBindings(), lookup, time.Hour)

	for range 10 {
		_, _, ok := c.Resolve(context.Background(), "acme", "orders")
		assert.False(t, ok)
	}
	assert.EqualValues(t, 1, lookup.calls.Load(), "an outage costs one lookup per pair per TTL")
}

func TestMiss_SnapshotDefaultIsNotAMiss(t *testing.T) {
	store := persistencetest.NewMemoryBindings()
	store.AssignSilently("*", "orders", rule("default", 1))
	lookup := &fakeLookup{}
	c := startCache(t, store, lookup, time.Hour)

	r, match, ok := c.Resolve(context.Background(), "acme", "orders")
	require.True(t, ok)
	assert.Equal(t, "default", r.ID)
	assert.Equal(t, types.MatchDefault, match)
	assert.Zero(t, lookup.calls.Load())
}

func TestMiss_ReloadDropsCachedAnswers(t *testing.T) {
	store := persistencetest.NewMemoryBindings()
	lookup := &fakeLookup{}
	c := startCache(t, store, lookup, time.Hour)
	ctx := context.Background()

	_, _, ok := c.Resolve(ctx, "acme", "orders")
	require.False(t, ok)

	store.AssignSilently("acme", "orders", rule("r1", 1))
	require.NoError(t, c.Reload(ctx))
	_, _, ok = c.Resolve(ctx, "acme", "orders")
	assert.True(t, ok, "the snapshot wins over a cached miss")

	_, _, _ = c.Resolve(ctx, "globex", "orders")
	assert.EqualValues(t, 2, lookup.calls.Load(), "misses are looked up again after a reload")
}
