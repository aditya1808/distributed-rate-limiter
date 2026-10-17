package rule_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/testutils"
)

func validRule() map[string]any {
	return map[string]any{"tenant": "acme", "namespace": "orders", "algorithm": "token_bucket", "limit": 300, "period": "1m", "burst": 20}
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

	resp = app.Caller(http.MethodGet, "/v1/rules/"+id, nil)
	require.Equal(t, http.StatusOK, resp.Status)
	assert.Equal(t, `"1"`, resp.Header.Get("ETag"))

	list := app.Caller(http.MethodGet, "/v1/rules?tenant=acme", nil).Map(t)
	assert.EqualValues(t, 1, list["count"])
	assert.EqualValues(t, 0, app.Caller(http.MethodGet, "/v1/rules?tenant=globex", nil).Map(t)["count"])

	update := validRule()
	update["limit"] = 600
	resp = app.Do(http.MethodPut, "/v1/rules/"+id, update, testutils.CallerHeaders(map[string]string{"If-Match": `"1"`}))
	require.Equal(t, http.StatusOK, resp.Status, "body: %s", resp.Body)
	assert.Equal(t, `"2"`, resp.Header.Get("ETag"))
	assert.EqualValues(t, 600, resp.Map(t)["limit"])

	assert.Equal(t, http.StatusNoContent, app.Caller(http.MethodDelete, "/v1/rules/"+id, nil).Status)
	assert.Equal(t, http.StatusNotFound, app.Caller(http.MethodGet, "/v1/rules/"+id, nil).Status)
	assert.Equal(t, http.StatusNotFound, app.Caller(http.MethodDelete, "/v1/rules/"+id, nil).Status)
}

func TestRules_UpdateTakesEffectWithoutRestart(t *testing.T) {
	app := testutils.NewTestApp(t)
	r := validRule()
	r["limit"], r["period"], r["burst"] = 1, "1h", 1
	id := app.CreateRule(r)["id"].(string)

	assert.Equal(t, true, app.Check("acme", "orders", 0)["allowed"])
	assert.Equal(t, false, app.Check("acme", "orders", 0)["allowed"])

	r["burst"] = 5
	resp := app.Do(http.MethodPut, "/v1/rules/"+id, r, testutils.CallerHeaders(map[string]string{"If-Match": `"1"`}))
	require.Equal(t, http.StatusOK, resp.Status)
	body := app.Check("acme", "orders", 0)
	assert.Equal(t, true, body["allowed"])
	assert.EqualValues(t, 5, body["limit"])
}

func TestRules_OptimisticConcurrency(t *testing.T) {
	app := testutils.NewTestApp(t)
	id := app.CreateRule(validRule())["id"].(string)

	resp := app.Caller(http.MethodPut, "/v1/rules/"+id, validRule())
	assert.Equal(t, http.StatusPreconditionRequired, resp.Status)

	resp = app.Do(http.MethodPut, "/v1/rules/"+id, validRule(), testutils.CallerHeaders(map[string]string{"If-Match": `"7"`}))
	assert.Equal(t, http.StatusPreconditionFailed, resp.Status)

	resp = app.Do(http.MethodPut, "/v1/rules/"+id, validRule(), testutils.CallerHeaders(map[string]string{"If-Match": `W/"1"`}))
	assert.Equal(t, http.StatusOK, resp.Status, "weak ETags are accepted")

	resp = app.Do(http.MethodPut, "/v1/rules/r_missing", validRule(), testutils.CallerHeaders(map[string]string{"If-Match": `"1"`}))
	assert.Equal(t, http.StatusNotFound, resp.Status)
}

func TestRules_ScopeIsImmutable(t *testing.T) {
	app := testutils.NewTestApp(t)
	id := app.CreateRule(validRule())["id"].(string)
	moved := validRule()
	moved["tenant"] = "globex"
	resp := app.Do(http.MethodPut, "/v1/rules/"+id, moved, testutils.CallerHeaders(map[string]string{"If-Match": `"1"`}))
	assert.Equal(t, http.StatusBadRequest, resp.Status)
}

func TestRules_Conflict(t *testing.T) {
	app := testutils.NewTestApp(t)
	app.CreateRule(validRule())
	assert.Equal(t, http.StatusConflict, app.Caller(http.MethodPost, "/v1/rules", validRule()).Status)
}

func TestRules_Validation(t *testing.T) {
	app := testutils.NewTestApp(t)
	cases := map[string]map[string]any{
		"bad algorithm":          {"namespace": "orders", "algorithm": "leaky", "limit": 1, "period": "1s"},
		"bad period":             {"namespace": "orders", "algorithm": "token_bucket", "limit": 1, "period": "soon"},
		"period too short":       {"namespace": "orders", "algorithm": "token_bucket", "limit": 1, "period": "10ms"},
		"bad tenant":             {"tenant": "a b", "namespace": "orders", "algorithm": "token_bucket", "limit": 1, "period": "1s"},
		"missing limit":          {"namespace": "orders", "algorithm": "token_bucket", "period": "1s"},
		"burst window on bucket": {"namespace": "orders", "algorithm": "token_bucket", "limit": 1, "period": "1m", "burst_window": "1s"},
		"window over cap":        {"namespace": "orders", "algorithm": "sliding_window", "limit": 20000, "period": "1m"},
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			resp := app.Caller(http.MethodPost, "/v1/rules", body)
			assert.Equal(t, http.StatusBadRequest, resp.Status, "body: %s", resp.Body)
			assert.NotEmpty(t, resp.Map(t)["errors"])
		})
	}
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
