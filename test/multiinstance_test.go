// Package test holds cross-instance tests: several complete service instances
// (separate rule caches, Redis pools and HTTP servers) sharing one Redis.
package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aditya1808/distributed-rate-limiter/testutils"
)

func twoInstances(t *testing.T) (*testutils.TestApp, *testutils.TestApp) {
	t.Helper()
	a := testutils.NewTestApp(t, testutils.WithInstanceID("a"), testutils.WithRefresh(time.Hour))
	b := testutils.NewTestApp(t, testutils.WithInstanceID("b"), testutils.WithRefresh(time.Hour), testutils.WithPrefix(a.Prefix))
	return a, b
}

type checkResult struct {
	Allowed bool `json:"allowed"`
}

// TestMultiInstance_ExactAdmissionUnderContention alternates 500 concurrent
// requests across two instances. Refill is 1/hour, so exactly `burst` may pass.
func TestMultiInstance_ExactAdmissionUnderContention(t *testing.T) {
	for _, algorithm := range []string{"token_bucket", "sliding_window"} {
		t.Run(algorithm, func(t *testing.T) {
			a, b := twoInstances(t)
			const burst = 40
			rule := map[string]any{"tenant": "acme", "namespace": "orders", "algorithm": algorithm, "period": "1h"}
			if algorithm == "token_bucket" {
				rule["limit"], rule["burst"] = 1, burst
			} else {
				rule["limit"] = burst
			}
			a.CreateRule(rule)
			// b learns the rule through pub/sub, not a shared process.
			require.Eventually(t, func() bool { return ruleVisible(t, b, "acme", "orders") }, 2*time.Second, 10*time.Millisecond)

			client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 100}, Timeout: 10 * time.Second}
			urls := []string{a.BaseURL, b.BaseURL}
			var (
				allowed   atomic.Int64
				served    [2]atomic.Int64
				perServer [2]atomic.Int64
				wg        sync.WaitGroup
				start     = make(chan struct{})
				body, _   = json.Marshal(map[string]any{"tenant": "acme", "namespace": "orders"})
				requests  = 500
			)
			for i := 0; i < requests; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					resp, err := client.Post(urls[i%2]+"/v1/check", "application/json", bytes.NewReader(body))
					if err != nil {
						t.Errorf("request %d: %v", i, err)
						return
					}
					defer resp.Body.Close()
					var r checkResult
					if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
						t.Errorf("decode %d: %v", i, err)
						return
					}
					served[i%2].Add(1)
					if r.Allowed {
						allowed.Add(1)
						perServer[i%2].Add(1)
					}
				}(i)
			}
			close(start)
			wg.Wait()

			assert.EqualValues(t, burst, allowed.Load(), "two instances together must admit exactly the burst")
			// Which instance wins the tokens is scheduling-dependent; only the total is fixed.
			assert.EqualValues(t, requests/2, served[0].Load(), "instance a answered its share")
			assert.EqualValues(t, requests/2, served[1].Load(), "instance b answered its share")
			t.Logf("%s: allowed %d (a=%d, b=%d) of %d", algorithm, allowed.Load(), perServer[0].Load(), perServer[1].Load(), requests)
		})
	}
}

// TestMultiInstance_RuleChangePropagates updates a rule through one instance
// and checks the other enforces it, via pub/sub (cache refresh is disabled).
func TestMultiInstance_RuleChangePropagates(t *testing.T) {
	a, b := twoInstances(t)
	created := a.CreateRule(map[string]any{"tenant": "acme", "namespace": "orders", "algorithm": "token_bucket", "limit": 1, "period": "1h", "burst": 2})
	id := created["id"].(string)

	require.Eventually(t, func() bool { return ruleVisible(t, b, "acme", "orders") }, 2*time.Second, 10*time.Millisecond)
	assert.EqualValues(t, 2, b.Check("acme", "orders", 0)["limit"])

	update := map[string]any{"tenant": "acme", "namespace": "orders", "algorithm": "token_bucket", "limit": 1, "period": "1h", "burst": 7}
	resp := a.Do(http.MethodPut, "/v1/rules/"+id, update, map[string]string{
		"Authorization": "Bearer " + testutils.AdminToken, "If-Match": `"1"`,
	})
	require.Equal(t, http.StatusOK, resp.Status, "body: %s", resp.Body)

	require.Eventually(t, func() bool {
		return b.Check("acme", "orders", 0)["limit"] == float64(7)
	}, 2*time.Second, 10*time.Millisecond, "instance b enforces the new limit")

	assert.Equal(t, http.StatusNoContent, b.Admin(http.MethodDelete, "/v1/rules/"+id, nil).Status)
	require.Eventually(t, func() bool {
		return a.Check("acme", "orders", 0)["reason"] == "no_rule"
	}, 2*time.Second, 10*time.Millisecond, "instance a drops the deleted rule")
}

func ruleVisible(t *testing.T, app *testutils.TestApp, tenant, namespace string) bool {
	t.Helper()
	resp := app.Do(http.MethodGet, fmt.Sprintf("/v1/quota/%s/%s", tenant, namespace), nil, nil)
	return resp.Status == http.StatusOK
}
