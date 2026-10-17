package assignment_test

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
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

func body(tenant, namespace, ruleID string) map[string]any {
	return map[string]any{"tenant": tenant, "namespace": namespace, "rule_id": ruleID}
}

func ifMatch(v string) map[string]string {
	return testutils.CallerHeaders(map[string]string{"If-Match": v})
}

func TestAssignments_Lifecycle(t *testing.T) {
	app := testutils.NewTestApp(t)
	r1, r2 := newRule(t, app, 2), newRule(t, app, 9)

	resp := app.Caller(http.MethodPost, "/v1/assignments", body("acme", "orders", r1))
	require.Equal(t, http.StatusCreated, resp.Status, "body: %s", resp.Body)
	created := resp.Map(t)
	id := created["id"].(string)
	assert.Regexp(t, `^a_[0-9a-z]{26}$`, id)
	assert.Equal(t, "/v1/assignments/"+id, resp.Header.Get("Location"))
	assert.Equal(t, `"1"`, resp.Header.Get("ETag"))
	assert.EqualValues(t, 1, created["version"])

	got := app.Caller(http.MethodGet, "/v1/assignments/"+id, nil)
	require.Equal(t, http.StatusOK, got.Status)
	assert.Equal(t, r1, got.Map(t)["rule_id"])
	assert.Equal(t, `"1"`, got.Header.Get("ETag"))

	resp = app.Do(http.MethodPut, "/v1/assignments/"+id, body("acme", "orders", r2), ifMatch(`"1"`))
	require.Equal(t, http.StatusOK, resp.Status, "body: %s", resp.Body)
	assert.Equal(t, `"2"`, resp.Header.Get("ETag"))
	assert.Equal(t, r2, resp.Map(t)["rule_id"])
	assert.Equal(t, created["created_at"], resp.Map(t)["created_at"])

	app.Assign("*", "orders", r1)
	app.Assign("globex", "search", r1)
	count := func(q string) any { return app.Caller(http.MethodGet, "/v1/assignments"+q, nil).Map(t)["count"] }
	assert.EqualValues(t, 3, count(""))
	assert.EqualValues(t, 2, count("?namespace=orders"))
	assert.EqualValues(t, 2, count("?rule_id="+r1))
	assert.EqualValues(t, 1, count("?tenant=*"))
	assert.EqualValues(t, 1, count("?tenant=acme&namespace=orders"), "list doubles as lookup by pair")

	assert.Equal(t, http.StatusNoContent, app.Caller(http.MethodDelete, "/v1/assignments/"+id, nil).Status)
	assert.Equal(t, http.StatusNotFound, app.Caller(http.MethodGet, "/v1/assignments/"+id, nil).Status)
	assert.Equal(t, http.StatusNotFound, app.Caller(http.MethodDelete, "/v1/assignments/"+id, nil).Status)
}

func TestAssignments_OnePerPair(t *testing.T) {
	app := testutils.NewTestApp(t)
	r := newRule(t, app, 1)
	app.Assign("acme", "orders", r)

	resp := app.Caller(http.MethodPost, "/v1/assignments", body("acme", "orders", r))
	assert.Equal(t, http.StatusConflict, resp.Status)
	assert.Equal(t, "urn:ratelimiter:problem:assignment-exists", resp.Map(t)["type"])

	other := app.Assign("acme", "search", r)["id"].(string)
	resp = app.Do(http.MethodPut, "/v1/assignments/"+other, body("acme", "orders", r), ifMatch(`"1"`))
	assert.Equal(t, http.StatusConflict, resp.Status, "moving onto a taken pair conflicts too")
}

func TestAssignments_ConcurrentCreatesForOnePairCreateOnce(t *testing.T) {
	app := testutils.NewTestApp(t)
	r := newRule(t, app, 1)

	var created, conflicts atomic.Int64
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			switch s := app.Caller(http.MethodPost, "/v1/assignments", body("acme", "orders", r)).Status; s {
			case http.StatusCreated:
				created.Add(1)
			case http.StatusConflict:
				conflicts.Add(1)
			default:
				t.Errorf("status %d", s)
			}
		})
	}
	wg.Wait()
	assert.EqualValues(t, 1, created.Load())
	assert.EqualValues(t, 19, conflicts.Load())
}

func TestAssignments_OptimisticConcurrency(t *testing.T) {
	app := testutils.NewTestApp(t)
	r := newRule(t, app, 1)
	id := app.Assign("acme", "orders", r)["id"].(string)

	assert.Equal(t, http.StatusPreconditionRequired, app.Caller(http.MethodPut, "/v1/assignments/"+id, body("acme", "orders", r)).Status)
	assert.Equal(t, http.StatusPreconditionFailed, app.Do(http.MethodPut, "/v1/assignments/"+id, body("acme", "orders", r), ifMatch(`"5"`)).Status)
	assert.Equal(t, http.StatusNotFound, app.Do(http.MethodPut, "/v1/assignments/a_missing", body("acme", "orders", r), ifMatch(`"1"`)).Status)
}

func TestAssignments_Validation(t *testing.T) {
	app := testutils.NewTestApp(t)
	r := newRule(t, app, 1)

	resp := app.Caller(http.MethodPost, "/v1/assignments", body("acme", "orders", "r_missing"))
	assert.Equal(t, http.StatusBadRequest, resp.Status)
	assert.Equal(t, "rule_id", resp.Map(t)["errors"].([]any)[0].(map[string]any)["field"])

	assert.Equal(t, http.StatusBadRequest, app.Caller(http.MethodPost, "/v1/assignments", body("ac:me", "orders", r)).Status)
	assert.Equal(t, http.StatusBadRequest, app.Caller(http.MethodPost, "/v1/assignments", body("acme", "*", r)).Status,
		"only the tenant can be a wildcard")
	assert.Equal(t, http.StatusBadRequest, app.Caller(http.MethodPost, "/v1/assignments", map[string]any{"tenant": "acme"}).Status)
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

func TestAssignments_TenantBeatsDefaultAndChangesApplyImmediately(t *testing.T) {
	app := testutils.NewTestApp(t)
	small, big := newRule(t, app, 1), newRule(t, app, 50)
	app.Assign("*", "orders", small)

	assert.EqualValues(t, 1, app.Check("acme", "orders", 0)["limit"])
	id := app.Assign("acme", "orders", big)["id"].(string)
	assert.EqualValues(t, 50, app.Check("acme", "orders", 0)["limit"])
	assert.EqualValues(t, 1, app.Check("globex", "orders", 0)["limit"])

	q := app.Caller(http.MethodGet, "/v1/quota/acme/orders", nil).Map(t)
	assert.Equal(t, "tenant", q["rule"].(map[string]any)["matched"])

	resp := app.Do(http.MethodPut, "/v1/assignments/"+id, body("acme", "search", big), ifMatch(`"1"`))
	require.Equal(t, http.StatusOK, resp.Status, "body: %s", resp.Body)
	assert.EqualValues(t, 1, app.Check("acme", "orders", 0)["limit"], "acme/orders falls back to the default")
	assert.EqualValues(t, 50, app.Check("acme", "search", 0)["limit"], "the assignment moved to acme/search")

	require.Equal(t, http.StatusNoContent, app.Caller(http.MethodDelete, "/v1/assignments/"+id, nil).Status)
	assert.Equal(t, "no_rule", app.Check("acme", "search", 0)["reason"])
}

// An assignment written behind the cache's back (no reload, no change event,
// periodic reload disabled) is still found through the Redis/Postgres miss
// path, once the cached "no rule" answer expires.
func TestAssignments_MissFallsBackToRedisThenPostgres(t *testing.T) {
	app := testutils.NewTestApp(t, testutils.WithRefresh(time.Hour), testutils.WithMissTTL(300*time.Millisecond))
	r := newRule(t, app, 3)

	require.Equal(t, "no_rule", app.Check("acme", "orders", 0)["reason"])

	_, err := app.App.Postgres.Exec(context.Background(),
		`INSERT INTO assignments (id, version, tenant, namespace, rule_id, created_at, updated_at)
		 VALUES ('a_direct', 1, 'acme', 'orders', $1, now(), now())`, r)
	require.NoError(t, err)

	assert.Equal(t, "no_rule", app.Check("acme", "orders", 0)["reason"], "\"no rule\" is cached for RULE_MISS_TTL")
	require.Eventually(t, func() bool {
		return app.Check("acme", "orders", 0)["rule_id"] == r
	}, 3*time.Second, 50*time.Millisecond)
}
