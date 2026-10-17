package testutils

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/config"
	"github.com/aditya1808/distributed-rate-limiter/core"
)

const AdminToken = "test-admin-token-0123456789"

// CallerToken is a well-formed JWT for the decision endpoints:
// {"alg":"RS256","typ":"JWT"}.{"sub":"test-caller"}.<signature>
const CallerToken = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0LWNhbGxlciJ9.c2lnbmF0dXJl"

// TestApp is a complete service instance behind an httptest server.
type TestApp struct {
	App     *core.App
	Server  *httptest.Server
	BaseURL string
	Prefix  string
	client  *http.Client
	tb      testing.TB
}

type Option func(*config.Config)

// WithPrefix shares a key prefix between apps, i.e. makes them instances of
// the same deployment.
func WithPrefix(p string) Option { return func(c *config.Config) { c.KeyPrefix = p } }

func WithInstanceID(id string) Option { return func(c *config.Config) { c.InstanceID = id } }

func WithRefresh(d time.Duration) Option { return func(c *config.Config) { c.RuleRefreshInterval = d } }

func WithDefaultDeny() Option { return func(c *config.Config) { c.DefaultAllow = false } }

func WithRedisURL(u string) Option { return func(c *config.Config) { c.RedisURL = u } }

func WithRedisTimeout(d time.Duration) Option { return func(c *config.Config) { c.RedisTimeout = d } }

func NewTestApp(tb testing.TB, opts ...Option) *TestApp {
	tb.Helper()
	gin.SetMode(gin.TestMode)
	rdb := RedisClient(tb)

	cfg := config.Config{
		RedisURL:            RedisURL(tb),
		RedisTimeout:        500 * time.Millisecond,
		RedisPoolSize:       100,
		RuleRefreshInterval: time.Second,
		AdminToken:          AdminToken,
		DefaultAllow:        true,
		InstanceID:          "test",
		ExposeInstanceID:    true,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = UniquePrefix(tb, rdb)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	app, err := core.New(ctx, cfg, log)
	if err != nil {
		tb.Fatalf("start app: %v", err)
	}
	srv := httptest.NewServer(app.Public)
	tb.Cleanup(func() {
		srv.Close()
		_ = app.Close()
	})
	return &TestApp{
		App:     app,
		Server:  srv,
		BaseURL: srv.URL,
		Prefix:  cfg.KeyPrefix,
		client:  &http.Client{Transport: newSpecTransport(tb), Timeout: 10 * time.Second},
		tb:      tb,
	}
}

type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

func (r Response) JSON(tb testing.TB, v any) {
	tb.Helper()
	if err := json.Unmarshal(r.Body, v); err != nil {
		tb.Fatalf("decode response %q: %v", r.Body, err)
	}
}

func (r Response) Map(tb testing.TB) map[string]any {
	tb.Helper()
	var m map[string]any
	r.JSON(tb, &m)
	return m
}

// Do sends a request; body may be nil, a []byte/string (sent raw) or any
// value (JSON-encoded).
func (a *TestApp) Do(method, path string, body any, headers map[string]string) Response {
	a.tb.Helper()
	var reader io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		reader = bytes.NewReader(b)
	case string:
		reader = bytes.NewReader([]byte(b))
	default:
		data, err := json.Marshal(b)
		if err != nil {
			a.tb.Fatalf("encode body: %v", err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, a.BaseURL+path, reader)
	if err != nil {
		a.tb.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		a.tb.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return Response{Status: resp.StatusCode, Header: resp.Header, Body: data}
}

func AdminHeaders() map[string]string {
	return map[string]string{"Authorization": "Bearer " + AdminToken}
}

func (a *TestApp) Admin(method, path string, body any) Response {
	a.tb.Helper()
	return a.Do(method, path, body, AdminHeaders())
}

// CallerHeaders authenticates a request to the decision endpoints, merged
// with any extra headers.
func CallerHeaders(extra ...map[string]string) map[string]string {
	h := map[string]string{"Authorization": "Bearer " + CallerToken}
	for _, e := range extra {
		for k, v := range e {
			h[k] = v
		}
	}
	return h
}

// Caller sends a request authenticated for the decision endpoints.
func (a *TestApp) Caller(method, path string, body any) Response {
	a.tb.Helper()
	return a.Do(method, path, body, CallerHeaders())
}

// CreateRule creates a rule and fails the test unless it returns 201.
func (a *TestApp) CreateRule(rule map[string]any) map[string]any {
	a.tb.Helper()
	resp := a.Admin(http.MethodPost, "/v1/rules", rule)
	if resp.Status != http.StatusCreated {
		a.tb.Fatalf("create rule: status %d body %s", resp.Status, resp.Body)
	}
	return resp.Map(a.tb)
}

func (a *TestApp) Check(tenant, namespace string, cost int) map[string]any {
	a.tb.Helper()
	body := map[string]any{"tenant": tenant, "namespace": namespace}
	if cost > 0 {
		body["cost"] = cost
	}
	resp := a.Caller(http.MethodPost, "/v1/check", body)
	if resp.Status != http.StatusOK {
		a.tb.Fatalf("check: status %d body %s", resp.Status, resp.Body)
	}
	return resp.Map(a.tb)
}
