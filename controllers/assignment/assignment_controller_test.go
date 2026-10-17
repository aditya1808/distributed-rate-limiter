package assignment_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/testutils"
)

func newRule(t *testing.T, app *testutils.TestApp, burst int) string {
	t.Helper()
	return app.CreateRule(map[string]any{"algorithm": "token_bucket", "limit": 1, "period": "1h", "burst": burst})["id"].(string)
}

func TestAssignments_Lifecycle(t *testing.T) {
	app := testutils.NewTestApp(t)
	r1, r2 := newRule(t, app, 2), newRule(t, app, 9)

	resp := app.Caller(http.MethodPut, "/v1/assignments/acme/orders", map[string]any{"rule_id": r1})
	require.Equal(t, http.StatusCreated, resp.Status, "body: %s", resp.Body)
	assert.Equal(t, r1, resp.Map(t)["rule_id"])

	resp = app.Caller(http.MethodPut, "/v1/assignments/acme/orders", map[string]any{"rule_id": r2})
	require.Equal(t, http.StatusOK, resp.Status, "replacing is idempotent, not a conflict")

	got := app.Caller(http.MethodGet, "/v1/assignments/acme/orders", nil)
	require.Equal(t, http.StatusOK, got.Status)
	assert.Equal(t, r2, got.Map(t)["rule_id"])

	app.Assign("*", "orders", r1)
	app.Assign("globex", "search", r1)
	count := func(q string) any { return app.Caller(http.MethodGet, "/v1/assignments"+q, nil).Map(t)["count"] }
	assert.EqualValues(t, 3, count(""))
	assert.EqualValues(t, 2, count("?namespace=orders"))
	assert.EqualValues(t, 2, count("?rule_id="+r1))
	assert.EqualValues(t, 1, count("?tenant=*"))

	assert.Equal(t, http.StatusNoContent, app.Caller(http.MethodDelete, "/v1/assignments/acme/orders", nil).Status)
	assert.Equal(t, http.StatusNotFound, app.Caller(http.MethodGet, "/v1/assignments/acme/orders", nil).Status)
	assert.Equal(t, http.StatusNotFound, app.Caller(http.MethodDelete, "/v1/assignments/acme/orders", nil).Status)
}

func TestAssignments_Validation(t *testing.T) {
	app := testutils.NewTestApp(t)
	r := newRule(t, app, 1)

	resp := app.Caller(http.MethodPut, "/v1/assignments/acme/orders", map[string]any{"rule_id": "r_missing"})
	assert.Equal(t, http.StatusBadRequest, resp.Status)
	assert.Equal(t, "rule_id", resp.Map(t)["errors"].([]any)[0].(map[string]any)["field"])

	assert.Equal(t, http.StatusBadRequest, app.Caller(http.MethodPut, "/v1/assignments/ac:me/orders", map[string]any{"rule_id": r}).Status)
	assert.Equal(t, http.StatusBadRequest, app.Caller(http.MethodPut, "/v1/assignments/acme/*", map[string]any{"rule_id": r}).Status,
		"only the tenant can be a wildcard")
	assert.Equal(t, http.StatusBadRequest, app.Caller(http.MethodPut, "/v1/assignments/acme/orders", map[string]any{}).Status)
	assert.Equal(t, http.StatusUnauthorized, app.Do(http.MethodGet, "/v1/assignments", nil, nil).Status)
}

func TestAssignments_SharedRuleGivesEachPairItsOwnQuota(t *testing.T) {
	app := testutils.NewTestApp(t)
	r := newRule(t, app, 2)
	app.Assign("acme", "orders", r)
	app.Assign("globex", "orders", r)

	for range 2 {
		assert.Equal(t, true, app.Check("acme", "orders", 0)["allowed"])
	}
	assert.Equal(t, false, app.Check("acme", "orders", 0)["allowed"])
	assert.Equal(t, true, app.Check("globex", "orders", 0)["allowed"], "acme's usage does not consume globex's quota")
}

func TestAssignments_TenantBeatsDefaultAndReassignIsImmediate(t *testing.T) {
	app := testutils.NewTestApp(t)
	small, big := newRule(t, app, 1), newRule(t, app, 50)
	app.Assign("*", "orders", small)

	assert.EqualValues(t, 1, app.Check("acme", "orders", 0)["limit"])
	app.Assign("acme", "orders", big)
	assert.EqualValues(t, 50, app.Check("acme", "orders", 0)["limit"])
	assert.EqualValues(t, 1, app.Check("globex", "orders", 0)["limit"])

	q := app.Caller(http.MethodGet, "/v1/quota/acme/orders", nil).Map(t)
	assert.Equal(t, "tenant", q["rule"].(map[string]any)["matched"])
}

// An assignment written behind the cache's back (no reload, no change event,
// periodic reload disabled) is still found through the Redis/Postgres miss
// path, once the cached "no rule" answer expires.
func TestAssignments_MissFallsBackToRedisThenPostgres(t *testing.T) {
	app := testutils.NewTestApp(t, testutils.WithRefresh(time.Hour), testutils.WithMissTTL(300*time.Millisecond))
	r := newRule(t, app, 3)

	body := app.Check("acme", "orders", 0)
	require.Equal(t, "no_rule", body["reason"])

	_, err := app.App.Postgres.Exec(context.Background(),
		`INSERT INTO assignments (tenant, namespace, rule_id, created_at, updated_at) VALUES ('acme', 'orders', $1, now(), now())`, r)
	require.NoError(t, err)

	assert.Equal(t, "no_rule", app.Check("acme", "orders", 0)["reason"], "\"no rule\" is cached for RULE_MISS_TTL")
	require.Eventually(t, func() bool {
		return app.Check("acme", "orders", 0)["rule_id"] == r
	}, 3*time.Second, 50*time.Millisecond)
}
