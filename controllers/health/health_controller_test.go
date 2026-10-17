package health_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aditya1808/distributed-rate-limiter/testutils"
)

func TestHealth(t *testing.T) {
	app := testutils.NewTestApp(t, testutils.WithInstanceID("probe-test"))

	live := app.Do(http.MethodGet, "/healthz", nil, nil)
	assert.Equal(t, http.StatusOK, live.Status)
	assert.Equal(t, "probe-test", live.Map(t)["instance"])

	ready := app.Do(http.MethodGet, "/readyz", nil, nil)
	assert.Equal(t, http.StatusOK, ready.Status)
	checks := ready.Map(t)["checks"].(map[string]any)
	assert.Equal(t, "up", checks["redis"])
	assert.Equal(t, true, checks["rules_loaded"])

	// Shutdown flips readiness first so the load balancer drains this instance.
	app.App.ShuttingDown.Store(true)
	ready = app.Do(http.MethodGet, "/readyz", nil, nil)
	assert.Equal(t, http.StatusServiceUnavailable, ready.Status)
	assert.Equal(t, http.StatusOK, app.Do(http.MethodGet, "/healthz", nil, nil).Status, "liveness is unaffected")
}
