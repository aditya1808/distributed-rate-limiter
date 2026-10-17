package feedback_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/testutils"
)

func TestFeedback_TightensAndRelaxesLimits(t *testing.T) {
	app := testutils.NewTestApp(t)
	app.CreateRule(map[string]any{"tenant": "acme", "namespace": "orders", "algorithm": "token_bucket", "limit": 1, "period": "1h", "burst": 10})

	resp := app.Caller(http.MethodPost, "/v1/feedback", map[string]any{"tenant": "acme", "namespace": "orders", "error_rate": 0.3})
	require.Equal(t, http.StatusOK, resp.Status, "body: %s", resp.Body)
	fb := resp.Map(t)
	assert.Equal(t, 0.5, fb["adaptive_multiplier"])
	assert.NotEmpty(t, fb["expires_at"])

	body := app.Check("acme", "orders", 0)
	assert.EqualValues(t, 5, body["limit"], "capacity halved")
	assert.Equal(t, 0.5, body["adaptive_multiplier"])

	resp = app.Caller(http.MethodPost, "/v1/feedback", map[string]any{"tenant": "acme", "namespace": "orders", "error_rate": 0})
	require.Equal(t, http.StatusOK, resp.Status)
	assert.Equal(t, 1.0, resp.Map(t)["adaptive_multiplier"])
	assert.EqualValues(t, 10, app.Check("acme", "orders", 0)["limit"])
}

func TestFeedback_Validation(t *testing.T) {
	app := testutils.NewTestApp(t)
	cases := map[string]map[string]any{
		"missing error rate": {"tenant": "acme", "namespace": "orders"},
		"error rate above 1": {"tenant": "acme", "namespace": "orders", "error_rate": 1.5},
		"ttl too short":      {"tenant": "acme", "namespace": "orders", "error_rate": 0.5, "ttl_seconds": 5},
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, http.StatusBadRequest, app.Caller(http.MethodPost, "/v1/feedback", body).Status)
		})
	}
	assert.Equal(t, http.StatusUnauthorized,
		app.Do(http.MethodPost, "/v1/feedback", map[string]any{"tenant": "a", "namespace": "b", "error_rate": 1}, nil).Status)
}
