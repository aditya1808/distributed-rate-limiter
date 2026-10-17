package dashboard_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/testutils"
)

func TestActiveTenants(t *testing.T) {
	app := testutils.NewTestApp(t)
	app.CreateRule(map[string]any{"namespace": "orders", "algorithm": "token_bucket", "limit": 1, "period": "1h", "burst": 10})
	app.Check("acme", "orders", 3)
	app.Check("globex", "orders", 1)

	var items []any
	require.Eventually(t, func() bool {
		resp := app.Caller(http.MethodGet, "/v1/tenants/active", nil)
		if resp.Status != http.StatusOK {
			return false
		}
		items, _ = resp.Map(t)["items"].([]any)
		return len(items) == 2
	}, 3*time.Second, 100*time.Millisecond, "activity is flushed in the background every second")

	used := map[string]float64{}
	for _, it := range items {
		m := it.(map[string]any)
		used[m["tenant"].(string)] = m["quota"].(map[string]any)["used"].(float64)
	}
	assert.Equal(t, map[string]float64{"acme": 3, "globex": 1}, used)

	assert.Equal(t, http.StatusBadRequest, app.Caller(http.MethodGet, "/v1/tenants/active?limit=500", nil).Status)
	assert.Equal(t, http.StatusUnauthorized, app.Do(http.MethodGet, "/v1/tenants/active", nil, nil).Status)
}

func TestDashboard_PublicServerRendered(t *testing.T) {
	app := testutils.NewTestApp(t)
	app.CreateRule(map[string]any{"namespace": "orders", "algorithm": "token_bucket", "limit": 1, "period": "1h", "burst": 10})
	app.Check("acme", "orders", 4)
	app.Check("globex", "unruled", 1)

	var html string
	require.Eventually(t, func() bool {
		resp := app.Do(http.MethodGet, "/dashboard", nil, nil)
		html = string(resp.Body)
		return resp.Status == http.StatusOK && strings.Contains(html, "globex")
	}, 3*time.Second, 100*time.Millisecond, "no credentials needed")

	assert.Contains(t, html, `http-equiv="refresh"`)
	assert.Contains(t, html, "4 / 10 (40%)")
	assert.Contains(t, html, "no rule")
	assert.NotContains(t, html, "<script")
}
