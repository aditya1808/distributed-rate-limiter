package rule_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/testutils"
)

func validRule() map[string]any {
	return map[string]any{"name": "orders standard", "algorithm": "token_bucket", "limit": 300, "period": "1m", "burst": 20}
}

func ifMatch(v string) map[string]string {
	return testutils.CallerHeaders(map[string]string{"If-Match": v})
}

func TestRules_Lifecycle(t *testing.T) {
	app := testutils.NewTestApp(t)

	resp := app.Caller(http.MethodPost, "/v1/rules", validRule())
	require.Equal(t, http.StatusCreated, resp.Status, "body: %s", resp.Body)
	created := resp.Map(t)
	id := created["id"].(string)
	assert.Equal(t, "/v1/rules/"+id, resp.Header.Get("Location"))
	assert.Equal(t, `"1"`, resp.Header.Get("ETag"))
	assert.EqualValues(t, 1, created["version"])
	assert.Equal(t, "1m", created["period"])
	assert.Equal(t, "orders standard", created["name"])
	assert.NotContains(t, created, "tenant", "rules have no scope; assignments do")

	resp = app.Caller(http.MethodGet, "/v1/rules/"+id, nil)
	require.Equal(t, http.StatusOK, resp.Status)
	assert.Equal(t, `"1"`, resp.Header.Get("ETag"))
	assert.EqualValues(t, 1, app.Caller(http.MethodGet, "/v1/rules", nil).Map(t)["count"])

	update := validRule()
	update["limit"], update["name"] = 600, "orders premium"
	resp = app.Do(http.MethodPut, "/v1/rules/"+id, update, ifMatch(`"1"`))
	require.Equal(t, http.StatusOK, resp.Status, "body: %s", resp.Body)
	assert.Equal(t, `"2"`, resp.Header.Get("ETag"))
	assert.EqualValues(t, 600, resp.Map(t)["limit"])
	assert.Equal(t, "orders premium", app.Caller(http.MethodGet, "/v1/rules/"+id, nil).Map(t)["name"])

	assert.Equal(t, http.StatusNoContent, app.Caller(http.MethodDelete, "/v1/rules/"+id, nil).Status)
	assert.Equal(t, http.StatusNotFound, app.Caller(http.MethodGet, "/v1/rules/"+id, nil).Status)
	assert.Equal(t, http.StatusNotFound, app.Caller(http.MethodDelete, "/v1/rules/"+id, nil).Status)
}

func TestRules_UpdateTakesEffectForEveryAssignedPair(t *testing.T) {
	app := testutils.NewTestApp(t)
	r := validRule()
	r["name"], r["limit"], r["period"], r["burst"] = "shared", 1, "1h", 1
	id := app.CreateRule(r)["id"].(string)
	app.Assign("acme", "orders", id)
	app.Assign("globex", "search", id)

	for _, p := range [][2]string{{"acme", "orders"}, {"globex", "search"}} {
		assert.Equal(t, true, app.Check(p[0], p[1], 0)["allowed"])
		assert.Equal(t, false, app.Check(p[0], p[1], 0)["allowed"])
	}

	r["burst"] = 5
	resp := app.Do(http.MethodPut, "/v1/rules/"+id, r, ifMatch(`"1"`))
	require.Equal(t, http.StatusOK, resp.Status)
	for _, p := range [][2]string{{"acme", "orders"}, {"globex", "search"}} {
		body := app.Check(p[0], p[1], 0)
		assert.Equal(t, true, body["allowed"], "%v starts fresh under the new version", p)
		assert.EqualValues(t, 5, body["limit"])
	}
}

func TestRules_OptimisticConcurrency(t *testing.T) {
	app := testutils.NewTestApp(t)
	id := app.CreateRule(validRule())["id"].(string)

	resp := app.Caller(http.MethodPut, "/v1/rules/"+id, validRule())
	assert.Equal(t, http.StatusPreconditionRequired, resp.Status)

	resp = app.Do(http.MethodPut, "/v1/rules/"+id, validRule(), ifMatch(`"7"`))
	assert.Equal(t, http.StatusPreconditionFailed, resp.Status)

	resp = app.Do(http.MethodPut, "/v1/rules/"+id, validRule(), ifMatch(`W/"1"`))
	assert.Equal(t, http.StatusOK, resp.Status, "weak ETags are accepted")

	resp = app.Do(http.MethodPut, "/v1/rules/r_missing", validRule(), ifMatch(`"1"`))
	assert.Equal(t, http.StatusNotFound, resp.Status)
}

func TestRules_CannotDeleteWhileAssigned(t *testing.T) {
	app := testutils.NewTestApp(t)
	id := app.CreateRule(validRule())["id"].(string)
	assignment := app.Assign("acme", "orders", id)["id"].(string)

	resp := app.Caller(http.MethodDelete, "/v1/rules/"+id, nil)
	assert.Equal(t, http.StatusConflict, resp.Status)
	assert.Equal(t, "urn:ratelimiter:problem:rule-in-use", resp.Map(t)["type"])

	require.Equal(t, http.StatusNoContent, app.Caller(http.MethodDelete, "/v1/assignments/"+assignment, nil).Status)
	assert.Equal(t, http.StatusNoContent, app.Caller(http.MethodDelete, "/v1/rules/"+id, nil).Status)
}

func TestRules_Validation(t *testing.T) {
	app := testutils.NewTestApp(t)
	cases := map[string]map[string]any{
		"missing name":           {"algorithm": "token_bucket", "limit": 1, "period": "1s"},
		"blank name":             {"name": "  ", "algorithm": "token_bucket", "limit": 1, "period": "1s"},
		"name too long":          {"name": strings.Repeat("n", 129), "algorithm": "token_bucket", "limit": 1, "period": "1s"},
		"bad algorithm":          {"name": "r", "algorithm": "leaky", "limit": 1, "period": "1s"},
		"bad period":             {"name": "r", "algorithm": "token_bucket", "limit": 1, "period": "soon"},
		"period too short":       {"name": "r", "algorithm": "token_bucket", "limit": 1, "period": "10ms"},
		"missing limit":          {"name": "r", "algorithm": "token_bucket", "period": "1s"},
		"burst window on bucket": {"name": "r", "algorithm": "token_bucket", "limit": 1, "period": "1m", "burst_window": "1s"},
		"window over cap":        {"name": "r", "algorithm": "sliding_window", "limit": 20000, "period": "1m"},
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			resp := app.Caller(http.MethodPost, "/v1/rules", body)
			assert.Equal(t, http.StatusBadRequest, resp.Status, "body: %s", resp.Body)
			assert.NotEmpty(t, resp.Map(t)["errors"])
		})
	}

	resp := app.Caller(http.MethodPost, "/v1/rules", map[string]any{"algorithm": "token_bucket", "limit": 1, "period": "1s"})
	assert.Equal(t, "name", resp.Map(t)["errors"].([]any)[0].(map[string]any)["field"])

	scoped := validRule()
	scoped["tenant"] = "acme"
	resp = app.Do(http.MethodPost, "/v1/rules", scoped, testutils.CallerHeaders())
	assert.Equal(t, http.StatusBadRequest, resp.Status, "scope belongs on an assignment, not the rule")
}

func TestRules_RequireJWT(t *testing.T) {
	app := testutils.NewTestApp(t)
	resp := app.Do(http.MethodGet, "/v1/rules", nil, nil)
	assert.Equal(t, http.StatusUnauthorized, resp.Status)
	assert.Contains(t, resp.Header.Get("WWW-Authenticate"), "Bearer")

	resp = app.Do(http.MethodGet, "/v1/rules", nil, map[string]string{"Authorization": "Bearer opaque-token-0000000000"})
	assert.Equal(t, http.StatusUnauthorized, resp.Status, "only JWTs are accepted")

	assert.Equal(t, http.StatusOK, app.Caller(http.MethodGet, "/v1/rules", nil).Status)
}
