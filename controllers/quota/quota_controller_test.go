package quota_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/testutils"
)

func TestQuota_PeekDoesNotConsume(t *testing.T) {
	app := testutils.NewTestApp(t)
	app.CreateRule(map[string]any{"tenant": "acme", "namespace": "orders", "algorithm": "token_bucket", "limit": 1, "period": "1h", "burst": 5})
	app.Check("acme", "orders", 2)

	for i := 0; i < 3; i++ {
		resp := app.Caller(http.MethodGet, "/v1/quota/acme/orders", nil)
		require.Equal(t, http.StatusOK, resp.Status, "body: %s", resp.Body)
		q := resp.Map(t)
		assert.EqualValues(t, 5, q["limit"])
		assert.EqualValues(t, 2, q["used"])
		assert.EqualValues(t, 3, q["remaining"])
		rule := q["rule"].(map[string]any)
		assert.Equal(t, "tenant", rule["matched"])
		assert.Equal(t, "1h", rule["period"])
	}
	assert.EqualValues(t, 2, app.Check("acme", "orders", 0)["remaining"], "peeks did not consume")
}

func TestQuota_DefaultRuleMatch(t *testing.T) {
	app := testutils.NewTestApp(t)
	app.CreateRule(map[string]any{"namespace": "orders", "algorithm": "sliding_window", "limit": 10, "period": "1m", "burst": 2, "burst_window": "1s"})
	resp := app.Caller(http.MethodGet, "/v1/quota/globex/orders", nil)
	require.Equal(t, http.StatusOK, resp.Status)
	q := resp.Map(t)
	rule := q["rule"].(map[string]any)
	assert.Equal(t, "default", rule["matched"])
	assert.Equal(t, "1s", rule["burst_window"])
	assert.EqualValues(t, 2, q["remaining"], "burst cap bounds remaining")
}

func TestQuota_Errors(t *testing.T) {
	app := testutils.NewTestApp(t)
	assert.Equal(t, http.StatusUnauthorized, app.Do(http.MethodGet, "/v1/quota/acme/orders", nil, nil).Status, "JWT required")
	assert.Equal(t, http.StatusNotFound, app.Caller(http.MethodGet, "/v1/quota/acme/orders", nil).Status)
	assert.Equal(t, http.StatusBadRequest, app.Caller(http.MethodGet, "/v1/quota/ac%3Ame/orders", nil).Status)

	app.CreateRule(map[string]any{"tenant": "acme", "namespace": "orders", "algorithm": "token_bucket", "limit": 1, "period": "1s"})
	require.NoError(t, app.App.Redis.Close())
	assert.Equal(t, http.StatusServiceUnavailable, app.Caller(http.MethodGet, "/v1/quota/acme/orders", nil).Status,
		"an observation has no safe fallback when the store is down")
}
