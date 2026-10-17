package pgstore_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/persistence"
	"github.com/aditya1808/distributed-rate-limiter/persistence/pgstore"
	"github.com/aditya1808/distributed-rate-limiter/testutils"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

func newStore(t *testing.T) *pgstore.Store {
	return pgstore.New(testutils.PostgresPool(t))
}

func newRule(id string) types.Rule {
	now := time.Now().UTC().Truncate(time.Microsecond)
	return types.Rule{ID: id, Name: "rule " + id, Algorithm: types.AlgorithmSlidingWindow, Limit: 100, Period: time.Minute,
		Burst: 10, BurstWindow: time.Second, FailOpen: true, Version: 1, CreatedAt: now, UpdatedAt: now}
}

func newAssignment(id, tenant, namespace, ruleID string) types.Assignment {
	now := time.Now().UTC().Truncate(time.Microsecond)
	return types.Assignment{ID: id, Version: 1, Tenant: tenant, Namespace: namespace, RuleID: ruleID, CreatedAt: now, UpdatedAt: now}
}

func assign(t *testing.T, s *pgstore.Store, tenant, namespace, ruleID string) string {
	t.Helper()
	id := "a_" + tenant + "_" + namespace
	require.NoError(t, s.CreateAssignment(context.Background(), newAssignment(id, tenant, namespace, ruleID)))
	return id
}

func TestRules_CRUDRoundTripsEveryField(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	r := newRule("r1")
	require.NoError(t, s.CreateRule(ctx, r))

	got, err := s.GetRule(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, r, got)

	r.Limit, r.Version = 200, 2
	require.NoError(t, s.UpdateRule(ctx, r, 1))
	got, _ = s.GetRule(ctx, "r1")
	assert.EqualValues(t, 200, got.Limit)

	assert.ErrorIs(t, s.UpdateRule(ctx, r, 1), types.ErrVersionMismatch)
	missing := newRule("nope")
	assert.ErrorIs(t, s.UpdateRule(ctx, missing, 1), types.ErrNotFound)

	list, err := s.ListRules(ctx)
	require.NoError(t, err)
	assert.Len(t, list, 1)

	require.NoError(t, s.DeleteRule(ctx, "r1"))
	_, err = s.GetRule(ctx, "r1")
	assert.ErrorIs(t, err, types.ErrNotFound)
	assert.ErrorIs(t, s.DeleteRule(ctx, "r1"), types.ErrNotFound)
}

func TestRules_ConcurrentUpdatesOnlyOneWins(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	require.NoError(t, s.CreateRule(ctx, newRule("r1")))

	var wins, mismatches atomic.Int64
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			r := newRule("r1")
			r.Limit, r.Version = int64(1000+i), 2
			switch err := s.UpdateRule(ctx, r, 1); {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, types.ErrVersionMismatch):
				mismatches.Add(1)
			default:
				t.Errorf("update: %v", err)
			}
		})
	}
	wg.Wait()
	assert.EqualValues(t, 1, wins.Load())
	assert.EqualValues(t, 19, mismatches.Load())
}

func TestRules_CannotDeleteAssignedRule(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	require.NoError(t, s.CreateRule(ctx, newRule("r1")))
	id := assign(t, s, "acme", "orders", "r1")

	assert.ErrorIs(t, s.DeleteRule(ctx, "r1"), types.ErrRuleInUse)
	_, err := s.DeleteAssignment(ctx, id)
	require.NoError(t, err)
	assert.NoError(t, s.DeleteRule(ctx, "r1"))
}

func TestAssignments_CRUDRoundTripsEveryField(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	require.NoError(t, s.CreateRule(ctx, newRule("r1")))
	require.NoError(t, s.CreateRule(ctx, newRule("r2")))

	a := newAssignment("a1", "acme", "orders", "r1")
	require.NoError(t, s.CreateAssignment(ctx, a))
	got, err := s.GetAssignment(ctx, "a1")
	require.NoError(t, err)
	assert.Equal(t, a, got)

	a.Tenant, a.Namespace, a.RuleID, a.Version = "globex", "search", "r2", 2
	require.NoError(t, s.UpdateAssignment(ctx, a, 1))
	got, _ = s.GetAssignment(ctx, "a1")
	assert.Equal(t, a, got)

	assert.ErrorIs(t, s.UpdateAssignment(ctx, a, 1), types.ErrVersionMismatch)
	assert.ErrorIs(t, s.UpdateAssignment(ctx, newAssignment("nope", "x", "y", "r1"), 1), types.ErrNotFound)

	deleted, err := s.DeleteAssignment(ctx, "a1")
	require.NoError(t, err)
	assert.Equal(t, "search", deleted.Namespace, "delete returns the pair so callers can invalidate it")
	_, err = s.DeleteAssignment(ctx, "a1")
	assert.ErrorIs(t, err, types.ErrNotFound)
	_, err = s.GetAssignment(ctx, "a1")
	assert.ErrorIs(t, err, types.ErrNotFound)
}

func TestAssignments_ConstraintErrors(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	require.NoError(t, s.CreateRule(ctx, newRule("r1")))
	assign(t, s, "acme", "orders", "r1")

	assert.ErrorIs(t, s.CreateAssignment(ctx, newAssignment("a2", "acme", "orders", "r1")), types.ErrAssignmentExists)
	assert.ErrorIs(t, s.CreateAssignment(ctx, newAssignment("a3", "acme", "search", "missing")), types.ErrUnknownRule)

	moved := newAssignment("a4", "acme", "search", "r1")
	require.NoError(t, s.CreateAssignment(ctx, moved))
	moved.Namespace, moved.Version = "orders", 2
	assert.ErrorIs(t, s.UpdateAssignment(ctx, moved, 1), types.ErrAssignmentExists)
	moved.Namespace, moved.RuleID = "search", "missing"
	assert.ErrorIs(t, s.UpdateAssignment(ctx, moved, 1), types.ErrUnknownRule)
}

func TestAssignments_ConcurrentCreatesForOnePairCreateOnce(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	require.NoError(t, s.CreateRule(ctx, newRule("r1")))

	var created, exists atomic.Int64
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			switch err := s.CreateAssignment(ctx, newAssignment("a"+strconv.Itoa(i), "acme", "orders", "r1")); {
			case err == nil:
				created.Add(1)
			case errors.Is(err, types.ErrAssignmentExists):
				exists.Add(1)
			default:
				t.Errorf("create: %v", err)
			}
		})
	}
	wg.Wait()
	assert.EqualValues(t, 1, created.Load())
	assert.EqualValues(t, 19, exists.Load())
}

func TestAssignments_ListFilters(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	require.NoError(t, s.CreateRule(ctx, newRule("r1")))
	require.NoError(t, s.CreateRule(ctx, newRule("r2")))
	assign(t, s, "acme", "orders", "r1")
	assign(t, s, "acme", "search", "r2")
	assign(t, s, "*", "orders", "r1")

	count := func(f persistence.AssignmentFilter) int {
		list, err := s.ListAssignments(ctx, f)
		require.NoError(t, err)
		return len(list)
	}
	assert.Equal(t, 3, count(persistence.AssignmentFilter{}))
	assert.Equal(t, 2, count(persistence.AssignmentFilter{Tenant: "acme"}))
	assert.Equal(t, 2, count(persistence.AssignmentFilter{Namespace: "orders"}))
	assert.Equal(t, 2, count(persistence.AssignmentFilter{RuleID: "r1"}))
	assert.Equal(t, 1, count(persistence.AssignmentFilter{Tenant: "acme", RuleID: "r1"}))
}

func TestResolve_TenantThenDefaultThenNone(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	require.NoError(t, s.CreateRule(ctx, newRule("tenant-rule")))
	require.NoError(t, s.CreateRule(ctx, newRule("default-rule")))
	assign(t, s, "*", "orders", "default-rule")
	assign(t, s, "acme", "orders", "tenant-rule")

	res, err := s.Resolve(ctx, "acme", "orders")
	require.NoError(t, err)
	assert.True(t, res.Found)
	assert.Equal(t, "tenant-rule", res.Rule.ID)
	assert.Equal(t, types.MatchTenant, res.Match)

	res, err = s.Resolve(ctx, "globex", "orders")
	require.NoError(t, err)
	assert.Equal(t, "default-rule", res.Rule.ID)
	assert.Equal(t, types.MatchDefault, res.Match)

	res, err = s.Resolve(ctx, "acme", "payments")
	require.NoError(t, err)
	assert.False(t, res.Found)
}

func TestBindings_JoinEveryAssignmentWithItsRule(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	require.NoError(t, s.CreateRule(ctx, newRule("shared")))
	for i := range 3 {
		assign(t, s, "tenant"+strconv.Itoa(i), "orders", "shared")
	}
	bindings, err := s.Bindings(ctx)
	require.NoError(t, err)
	require.Len(t, bindings, 3)
	for _, b := range bindings {
		assert.Equal(t, "shared", b.Rule.ID)
		assert.Equal(t, "orders", b.Namespace)
	}
}

func TestMigrate_IsIdempotentAndConcurrencySafe(t *testing.T) {
	pool := testutils.PostgresPool(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() { assert.NoError(t, pgstore.Migrate(context.Background(), pool, log)) })
	}
	wg.Wait()
	files, err := os.ReadDir("migrations")
	require.NoError(t, err)
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM schema_migrations`).Scan(&n))
	assert.Equal(t, len(files), n)
}
