package check_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/testutils"
)

func TestCheck_AllowsThenDenies(t *testing.T) {
	app := testutils.NewTestApp(t)
	app.CreateRule(map[string]any{"tenant": "acme", "namespace": "orders", "algorithm": "token_bucket", "limit": 1, "period": "1h", "burst": 3})

	for i := 2; i >= 0; i-- {
		body := app.Check("acme", "orders", 0)
		assert.Equal(t, true, body["allowed"])
		assert.EqualValues(t, i, body["remaining"])
		assert.EqualValues(t, 3, body["limit"])
		assert.Equal(t, "token_bucket", body["algorithm"])
		assert.NotEmpty(t, body["reset_at"])
	}
	body := app.Check("acme", "orders", 0)
	assert.Equal(t, false, body["allowed"])
	assert.Equal(t, "quota_exceeded", body["reason"])
	assert.Positive(t, body["retry_after_ms"])
}

func TestCheck_DefaultRuleIsPerTenant(t *testing.T) {
	app := testutils.NewTestApp(t)
	app.CreateRule(map[string]any{"namespace": "search", "algorithm": "sliding_window", "limit": 1, "period": "1h"})

	assert.Equal(t, true, app.Check("acme", "search", 0)["allowed"])
	assert.Equal(t, false, app.Check("acme", "search", 0)["allowed"])
	assert.Equal(t, true, app.Check("globex", "search", 0)["allowed"], "another tenant has its own quota under the * rule")
}

func TestCheck_NoRuleUsesDefaultPolicy(t *testing.T) {
	allowApp := testutils.NewTestApp(t)
	body := allowApp.Check("acme", "unknown", 0)
	assert.Equal(t, true, body["allowed"])
	assert.Equal(t, "no_rule", body["reason"])

	denyApp := testutils.NewTestApp(t, testutils.WithDefaultDeny())
	assert.Equal(t, false, denyApp.Check("acme", "unknown", 0)["allowed"])
}

func TestCheck_CostExceedingCapacity(t *testing.T) {
	app := testutils.NewTestApp(t)
	app.CreateRule(map[string]any{"tenant": "acme", "namespace": "orders", "algorithm": "token_bucket", "limit": 10, "period": "1s", "burst": 5})
	body := app.Check("acme", "orders", 6)
	assert.Equal(t, false, body["allowed"])
	assert.Equal(t, "cost_exceeds_capacity", body["reason"])
}

func TestCheck_Validation(t *testing.T) {
	app := testutils.NewTestApp(t)
	cases := []struct {
		name       string
		body       any
		wantStatus int
		wantField  string
	}{
		{"missing tenant", map[string]any{"namespace": "orders"}, http.StatusBadRequest, "tenant"},
		{"tenant with colon", map[string]any{"tenant": "a:b", "namespace": "orders"}, http.StatusBadRequest, "tenant"},
		{"tenant too long", map[string]any{"tenant": strings.Repeat("a", 65), "namespace": "orders"}, http.StatusBadRequest, "tenant"},
		{"cost too high", map[string]any{"tenant": "acme", "namespace": "orders", "cost": 1001}, http.StatusBadRequest, "cost"},
		{"cost wrong type", map[string]any{"tenant": "acme", "namespace": "orders", "cost": "two"}, http.StatusBadRequest, "cost"},
		{"unknown field", map[string]any{"tenant": "acme", "namespace": "orders", "extra": 1}, http.StatusBadRequest, ""},
		{"malformed json", "{not json", http.StatusBadRequest, ""},
		{"empty body", "", http.StatusBadRequest, ""},
		{"body too large", `{"tenant":"` + strings.Repeat("a", 70<<10) + `"}`, http.StatusRequestEntityTooLarge, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := app.Caller(http.MethodPost, "/v1/check", tc.body)
			require.Equal(t, tc.wantStatus, resp.Status, "body: %s", resp.Body)
			assert.Equal(t, "application/problem+json", resp.Header.Get("Content-Type"))
			p := resp.Map(t)
			assert.NotEmpty(t, p["instance"], "problem carries the request id")
			if tc.wantField != "" {
				errs, _ := p["errors"].([]any)
				require.NotEmpty(t, errs)
				assert.Equal(t, tc.wantField, errs[0].(map[string]any)["field"])
			}
		})
	}
}

func TestCheck_RequestAndInstanceHeaders(t *testing.T) {
	app := testutils.NewTestApp(t, testutils.WithInstanceID("instance-a"))
	resp := app.Do(http.MethodPost, "/v1/check", map[string]any{"tenant": "acme", "namespace": "orders"},
		testutils.CallerHeaders(map[string]string{"X-Request-ID": "trace-123"}))
	assert.Equal(t, "trace-123", resp.Header.Get("X-Request-ID"))
	assert.Equal(t, "instance-a", resp.Header.Get("X-Instance-ID"))

	resp = app.Do(http.MethodPost, "/v1/check", map[string]any{"tenant": "acme", "namespace": "orders"},
		testutils.CallerHeaders(map[string]string{"X-Request-ID": "bad id with spaces"}))
	assert.NotEqual(t, "bad id with spaces", resp.Header.Get("X-Request-ID"), "malformed ids are replaced")
}

func TestCheck_StoreDownAppliesFailMode(t *testing.T) {
	app := testutils.NewTestApp(t)
	app.CreateRule(map[string]any{"tenant": "open", "namespace": "orders", "algorithm": "token_bucket", "limit": 10, "period": "1s", "fail_open": true})
	app.CreateRule(map[string]any{"tenant": "closed", "namespace": "orders", "algorithm": "token_bucket", "limit": 10, "period": "1s"})

	// Close the app's Redis client: every store call now fails immediately.
	require.NoError(t, app.App.Redis.Close())

	open := app.Check("open", "orders", 0)
	assert.Equal(t, true, open["allowed"])
	assert.Equal(t, true, open["degraded"])
	assert.Equal(t, "store_unavailable", open["reason"])

	closed := app.Check("closed", "orders", 0)
	assert.Equal(t, false, closed["allowed"])
	assert.Equal(t, true, closed["degraded"])

	ready := app.Do(http.MethodGet, "/readyz", nil, nil)
	assert.Equal(t, http.StatusServiceUnavailable, ready.Status)
}

func TestCheck_RequiresJWT(t *testing.T) {
	app := testutils.NewTestApp(t)
	body := map[string]any{"tenant": "acme", "namespace": "orders"}

	resp := app.Do(http.MethodPost, "/v1/check", body, nil)
	assert.Equal(t, http.StatusUnauthorized, resp.Status)
	assert.Equal(t, "urn:ratelimiter:problem:unauthorized", resp.Map(t)["type"])

	resp = app.Do(http.MethodPost, "/v1/check", body, map[string]string{"Authorization": "Bearer not-a-jwt"})
	assert.Equal(t, http.StatusUnauthorized, resp.Status)
	assert.Equal(t, "urn:ratelimiter:problem:invalid-token", resp.Map(t)["type"])
}

func TestUnknownRouteAndMethod(t *testing.T) {
	app := testutils.NewTestApp(t)
	assert.Equal(t, http.StatusNotFound, app.Do(http.MethodGet, "/v1/nope", nil, nil).Status)
	assert.Equal(t, http.StatusMethodNotAllowed, app.Do(http.MethodGet, "/v1/check", nil, nil).Status)
}
