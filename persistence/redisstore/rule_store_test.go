package redisstore_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
	"github.com/aditya1808/distributed-rate-limiter/persistence/redisstore"
	"github.com/aditya1808/distributed-rate-limiter/testutils"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

func newStore(t *testing.T) (*redisstore.RuleStore, constants.Keyspace) {
	t.Helper()
	rdb := testutils.RedisClient(t)
	keys := constants.NewKeyspace(testutils.UniquePrefix(t, rdb))
	return redisstore.NewRuleStore(rdb, keys), keys
}

func rule(id, tenant string, version int64) types.Rule {
	return types.Rule{ID: id, Tenant: tenant, Namespace: "orders", Algorithm: types.AlgorithmTokenBucket,
		Limit: 10, Period: time.Second, Version: version, CreatedAt: time.Now().UTC()}
}

func TestRuleStore_CRUD(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	require.NoError(t, s.Create(ctx, rule("r1", "acme", 1)))
	got, err := s.Get(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, "acme", got.Tenant)
	assert.Equal(t, time.Second, got.Period)

	assert.ErrorIs(t, s.Create(ctx, rule("r2", "acme", 1)), types.ErrConflict, "one rule per tenant+namespace")

	updated := rule("r1", "acme", 2)
	updated.Limit = 99
	assert.ErrorIs(t, s.Update(ctx, updated, 5), types.ErrVersionMismatch)
	require.NoError(t, s.Update(ctx, updated, 1))
	got, _ = s.Get(ctx, "r1")
	assert.EqualValues(t, 99, got.Limit)

	all, err := s.List(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 1)

	require.NoError(t, s.Delete(ctx, "r1"))
	_, err = s.Get(ctx, "r1")
	assert.ErrorIs(t, err, types.ErrNotFound)
	assert.ErrorIs(t, s.Delete(ctx, "r1"), types.ErrNotFound)
	assert.ErrorIs(t, s.Update(ctx, updated, 2), types.ErrNotFound)

	require.NoError(t, s.Create(ctx, rule("r3", "acme", 1)), "delete frees the tenant+namespace slot")
}

func TestRuleStore_ConcurrentCreatesOnlyOneWins(t *testing.T) {
	s, _ := newStore(t)
	var wins, conflicts atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Create(context.Background(), rule("r-"+testutils.RandomSuffix(), "acme", 1))
			switch {
			case err == nil:
				wins.Add(1)
			case assert.ErrorIs(t, err, types.ErrConflict):
				conflicts.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, wins.Load())
	assert.EqualValues(t, 49, conflicts.Load())
}

func TestRuleStore_ConcurrentUpdatesOnlyOneWins(t *testing.T) {
	s, _ := newStore(t)
	require.NoError(t, s.Create(context.Background(), rule("r1", "acme", 1)))
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.Update(context.Background(), rule("r1", "acme", 2), 1) == nil {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, wins.Load(), "the version check is atomic with the write")
}

func TestRuleStore_WritesPublishChangeEvents(t *testing.T) {
	s, _ := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := s.Subscribe(ctx)
	require.NoError(t, err)

	require.NoError(t, s.Create(ctx, rule("r1", "acme", 1)))
	require.NoError(t, s.Update(ctx, rule("r1", "acme", 2), 1))
	require.NoError(t, s.Delete(ctx, "r1"))

	want := []persistence.ChangeEvent{
		{Op: persistence.ChangeUpsert, ID: "r1", Version: 1},
		{Op: persistence.ChangeUpsert, ID: "r1", Version: 2},
		{Op: persistence.ChangeDelete, ID: "r1"},
	}
	for _, w := range want {
		select {
		case got := <-events:
			assert.Equal(t, w, got)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %+v", w)
		}
	}
}
