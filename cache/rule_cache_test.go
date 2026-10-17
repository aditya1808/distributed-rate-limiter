package cache

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/metrics"
	"github.com/aditya1808/distributed-rate-limiter/persistence/persistencetest"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

func rule(id, tenant string, version int64) types.Rule {
	return types.Rule{ID: id, Tenant: tenant, Namespace: "orders", Algorithm: types.AlgorithmTokenBucket,
		Limit: 10, Period: time.Second, Version: version}
}

func startCache(t *testing.T, store *persistencetest.MemoryRuleStore, refresh time.Duration) *RuleCache {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	c := New(store, store, refresh, metrics.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, c.Start(ctx))
	t.Cleanup(func() {
		cancel()
		<-c.Done()
	})
	return c
}

func TestResolve_TenantRuleBeatsDefault(t *testing.T) {
	store := persistencetest.NewMemoryRuleStore()
	require.NoError(t, store.Create(context.Background(), rule("default", "*", 1)))
	require.NoError(t, store.Create(context.Background(), rule("acme", "acme", 1)))
	c := startCache(t, store, time.Hour)

	r, match, ok := c.Resolve("acme", "orders")
	require.True(t, ok)
	assert.Equal(t, "acme", r.ID)
	assert.Equal(t, types.MatchTenant, match)

	r, match, ok = c.Resolve("globex", "orders")
	require.True(t, ok)
	assert.Equal(t, "default", r.ID)
	assert.Equal(t, types.MatchDefault, match)

	_, _, ok = c.Resolve("acme", "payments")
	assert.False(t, ok)
	assert.True(t, c.Loaded())
}

func TestChangeEventsPropagate(t *testing.T) {
	store := persistencetest.NewMemoryRuleStore()
	c := startCache(t, store, time.Hour)
	ctx := context.Background()

	require.NoError(t, store.Create(ctx, rule("r1", "acme", 1)))
	require.Eventually(t, func() bool { _, _, ok := c.Resolve("acme", "orders"); return ok }, time.Second, 5*time.Millisecond)

	updated := rule("r1", "acme", 2)
	updated.Limit = 99
	require.NoError(t, store.Update(ctx, updated, 1))
	require.Eventually(t, func() bool { r, _, _ := c.Resolve("acme", "orders"); return r.Limit == 99 }, time.Second, 5*time.Millisecond)

	require.NoError(t, store.Delete(ctx, "r1"))
	require.Eventually(t, func() bool { _, _, ok := c.Resolve("acme", "orders"); return !ok }, time.Second, 5*time.Millisecond)
}

func TestUpsertNeverMovesBackwards(t *testing.T) {
	c := startCache(t, persistencetest.NewMemoryRuleStore(), time.Hour)
	newer := rule("r1", "acme", 5)
	newer.Limit = 50
	c.Upsert(newer)
	c.Upsert(rule("r1", "acme", 3))

	r, _, _ := c.Resolve("acme", "orders")
	assert.EqualValues(t, 5, r.Version)
	assert.EqualValues(t, 50, r.Limit)
}

func TestReloadKeepsNewerLocalVersion(t *testing.T) {
	store := persistencetest.NewMemoryRuleStore()
	store.PutSilently(rule("r1", "acme", 1))
	c := startCache(t, store, time.Hour)

	// This instance already applied v2 locally; a reload that reads v1 (e.g.
	// started before the write) must not regress it.
	c.Upsert(rule("r1", "acme", 2))
	require.NoError(t, c.Reload(context.Background()))
	r, _, _ := c.Resolve("acme", "orders")
	assert.EqualValues(t, 2, r.Version)
}

func TestPeriodicReloadRecoversLostEvents(t *testing.T) {
	store := persistencetest.NewMemoryRuleStore()
	c := startCache(t, store, 50*time.Millisecond)

	store.PutSilently(rule("r1", "acme", 1)) // no pub/sub message
	require.Eventually(t, func() bool { _, _, ok := c.Resolve("acme", "orders"); return ok }, 2*time.Second, 10*time.Millisecond)
}

func TestNewRuleForSamePairReplacesOld(t *testing.T) {
	c := startCache(t, persistencetest.NewMemoryRuleStore(), time.Hour)
	c.Upsert(rule("old", "acme", 1))
	c.Upsert(rule("new", "acme", 1))

	r, _, _ := c.Resolve("acme", "orders")
	assert.Equal(t, "new", r.ID)
	assert.Equal(t, 1, c.Len())
	c.Remove("old") // late delete for the replaced rule must not remove the new one
	_, _, ok := c.Resolve("acme", "orders")
	assert.True(t, ok)
}
