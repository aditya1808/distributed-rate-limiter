//go:build e2e

// Package e2e runs against a real deployment over HTTP only:
//
//	E2E_BASE_URLS      comma-separated base URLs (several instances, or one load balancer)
//	E2E_FRESH_CONNS    "true" opens a new connection per request so a load
//	                   balancer actually spreads requests across instances
//	E2E_MIN_INSTANCES  minimum distinct X-Instance-ID values that must serve traffic
//	E2E_JWT            JWT for every /v1 route (defaults to a well-formed test token)
package e2e

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultJWT is {"alg":"RS256","typ":"JWT"}.{"sub":"e2e"}.<signature>
const defaultJWT = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJlMmUifQ.c2lnbmF0dXJl"

type env struct {
	urls         []string
	jwt          string
	client       *http.Client
	minInstances int
}

func setup(t *testing.T) env {
	t.Helper()
	raw := os.Getenv("E2E_BASE_URLS")
	if raw == "" {
		t.Skip("E2E_BASE_URLS not set")
	}
	var urls []string
	for _, u := range strings.Split(raw, ",") {
		if u = strings.TrimRight(strings.TrimSpace(u), "/"); u != "" {
			urls = append(urls, u)
		}
	}
	minInstances, _ := strconv.Atoi(os.Getenv("E2E_MIN_INSTANCES"))
	transport := &http.Transport{MaxIdleConnsPerHost: 200, DisableKeepAlives: os.Getenv("E2E_FRESH_CONNS") == "true"}
	jwt := os.Getenv("E2E_JWT")
	if jwt == "" {
		jwt = defaultJWT
	}
	return env{urls: urls, jwt: jwt, client: &http.Client{Transport: transport, Timeout: 15 * time.Second}, minInstances: max(minInstances, 1)}
}

func (e env) do(t *testing.T, method, url string, body any, headers ...string) (int, http.Header, []byte) {
	t.Helper()
	var r io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		r = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, url, r)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.jwt)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := e.client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, data
}

func uniqueTenant(t *testing.T) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return "e2e-" + hex.EncodeToString(b)
}

// assignRule creates rule and assigns it to tenant in namespace. Cleanup runs
// in reverse, so the assignment is removed before the rule.
func (e env) assignRule(t *testing.T, tenant, namespace string, rule map[string]any) string {
	t.Helper()
	status, _, body := e.do(t, http.MethodPost, e.urls[0]+"/v1/rules", rule)
	require.Equal(t, http.StatusCreated, status, "create rule: %s", body)
	var created map[string]any
	require.NoError(t, json.Unmarshal(body, &created))
	id := created["id"].(string)
	t.Cleanup(func() { e.do(t, http.MethodDelete, e.urls[0]+"/v1/rules/"+id, nil) })

	path := fmt.Sprintf("%s/v1/assignments/%s/%s", e.urls[0], tenant, namespace)
	status, _, body = e.do(t, http.MethodPut, path, map[string]any{"rule_id": id})
	require.Equal(t, http.StatusCreated, status, "assign rule: %s", body)
	t.Cleanup(func() { e.do(t, http.MethodDelete, path, nil) })
	// Give every instance's cache time to receive the change event.
	time.Sleep(500 * time.Millisecond)
	return id
}

func TestE2E_Health(t *testing.T) {
	e := setup(t)
	for _, u := range e.urls {
		status, _, body := e.do(t, http.MethodGet, u+"/readyz", nil)
		assert.Equal(t, http.StatusOK, status, "%s/readyz: %s", u, body)
	}
}

func TestE2E_ExactAdmissionAcrossInstances(t *testing.T) {
	for _, algorithm := range []string{"token_bucket", "sliding_window"} {
		t.Run(algorithm, func(t *testing.T) {
			e := setup(t)
			tenant := uniqueTenant(t)
			const burst = 50
			rule := map[string]any{"algorithm": algorithm, "period": "1h"}
			if algorithm == "token_bucket" {
				rule["limit"], rule["burst"] = 1, burst
			} else {
				rule["limit"] = burst
			}
			e.assignRule(t, tenant, "orders", rule)

			const requests = 400
			var (
				allowed  atomic.Int64
				degraded atomic.Int64
				errs     atomic.Int64
				mu       sync.Mutex
				served   = map[string]int{}
				wg       sync.WaitGroup
				start    = make(chan struct{})
				body, _  = json.Marshal(map[string]any{"tenant": tenant, "namespace": "orders"})
			)
			for i := 0; i < requests; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					req, _ := http.NewRequest(http.MethodPost, e.urls[i%len(e.urls)]+"/v1/check", bytes.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Authorization", "Bearer "+e.jwt)
					resp, err := e.client.Do(req)
					if err != nil {
						errs.Add(1)
						return
					}
					defer resp.Body.Close()
					var r struct {
						Allowed  bool `json:"allowed"`
						Degraded bool `json:"degraded"`
					}
					if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&r) != nil {
						errs.Add(1)
						return
					}
					if r.Allowed {
						allowed.Add(1)
					}
					if r.Degraded {
						degraded.Add(1)
					}
					mu.Lock()
					served[resp.Header.Get("X-Instance-ID")]++
					mu.Unlock()
				}(i)
			}
			close(start)
			wg.Wait()

			t.Logf("allowed=%d degraded=%d errors=%d served_by=%v", allowed.Load(), degraded.Load(), errs.Load(), served)
			assert.Zero(t, errs.Load())
			assert.LessOrEqual(t, allowed.Load(), int64(burst), "instances together must never admit more than the burst")
			// A store call that times out is ambiguous: the script may still run
			// and consume a token while the caller gets a degraded deny. Each such
			// response can hide at most one admission, so the gap is bounded by it.
			assert.GreaterOrEqual(t, allowed.Load(), int64(burst)-degraded.Load(),
				"every admission short of the burst must be explained by a degraded response")
			assert.LessOrEqual(t, degraded.Load(), int64(requests/20), "degraded decisions must stay rare (<5%%)")
			assert.GreaterOrEqual(t, len(served), e.minInstances, "requests must be served by at least %d instances", e.minInstances)
		})
	}
}

func TestE2E_RuleUpdateVisibleEverywhere(t *testing.T) {
	e := setup(t)
	tenant := uniqueTenant(t)
	rule := map[string]any{"algorithm": "token_bucket", "limit": 1, "period": "1h", "burst": 3}
	id := e.assignRule(t, tenant, "orders", rule)

	rule["burst"] = 9
	status, _, body := e.do(t, http.MethodPut, e.urls[len(e.urls)-1]+"/v1/rules/"+id, rule, "If-Match", `"1"`)
	require.Equal(t, http.StatusOK, status, "update: %s", body)

	require.Eventually(t, func() bool {
		for i := 0; i < 10; i++ { // several requests so a load balancer hits every instance
			_, _, b := e.do(t, http.MethodGet, e.urls[i%len(e.urls)]+fmt.Sprintf("/v1/quota/%s/orders", tenant), nil)
			var q map[string]any
			if json.Unmarshal(b, &q) != nil || q["limit"] != float64(9) {
				return false
			}
		}
		return true
	}, 10*time.Second, 200*time.Millisecond, "every instance enforces the updated rule")
}

func TestE2E_RequiresJWT(t *testing.T) {
	e := setup(t)
	resp, err := e.client.Post(e.urls[0]+"/v1/check", "application/json",
		strings.NewReader(`{"tenant":"acme","namespace":"orders"}`))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	resp, err = e.client.Get(e.urls[0] + "/v1/rules")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	resp, err = e.client.Get(e.urls[0] + "/dashboard")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "the dashboard is public")
}
