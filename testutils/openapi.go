package testutils

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"
)

var (
	specOnce   sync.Once
	specRouter routers.Router
	specErr    error
)

// SpecPath is the API contract every HTTP test is validated against.
func SpecPath() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "apispec", "ratelimiter.openapi.yaml")
}

func loadSpec() (routers.Router, error) {
	specOnce.Do(func() {
		openapi3filter.RegisterBodyDecoder("text/html", openapi3filter.FileBodyDecoder)
		loader := openapi3.NewLoader()
		doc, err := loader.LoadFromFile(SpecPath())
		if err != nil {
			specErr = err
			return
		}
		if err := doc.Validate(context.Background()); err != nil {
			specErr = err
			return
		}
		specRouter, specErr = gorillamux.NewRouter(doc)
	})
	return specRouter, specErr
}

// specTransport validates every request and response against the OpenAPI
// spec, so the code cannot drift from the published contract unnoticed.
type specTransport struct {
	tb     testing.TB
	router routers.Router
	next   http.RoundTripper
}

func newSpecTransport(tb testing.TB) http.RoundTripper {
	tb.Helper()
	router, err := loadSpec()
	if err != nil {
		tb.Fatalf("load OpenAPI spec: %v", err)
	}
	return &specTransport{tb: tb, router: router, next: http.DefaultTransport}
}

func (t *specTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var reqBody []byte
	if req.Body != nil {
		reqBody, _ = io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewReader(reqBody))
	}

	// Match against the spec's server URL rather than the ephemeral test port.
	specReq := req.Clone(req.Context())
	specReq.URL = &url.URL{Scheme: "http", Host: "localhost:8080", Path: req.URL.Path, RawQuery: req.URL.RawQuery}
	specReq.Host = "localhost:8080"
	specReq.Body = io.NopCloser(bytes.NewReader(reqBody))

	route, pathParams, findErr := t.router.FindRoute(specReq)

	var input *openapi3filter.RequestValidationInput
	if findErr == nil {
		input = &openapi3filter.RequestValidationInput{
			Request:    specReq,
			PathParams: pathParams,
			Route:      route,
			Options: &openapi3filter.Options{
				AuthenticationFunc: openapi3filter.NoopAuthenticationFunc,
				MultiError:         true,
			},
		}
		// Invalid requests are sent on purpose by negative tests, so request
		// validation failures are not test failures; responses always must match.
		_ = openapi3filter.ValidateRequest(req.Context(), input)
	}

	resp, err := t.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(respBody))

	if findErr != nil {
		if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusMethodNotAllowed {
			t.tb.Errorf("OpenAPI: %s %s is not documented in the spec: %v", req.Method, req.URL.Path, findErr)
		}
		return resp, nil
	}

	err = openapi3filter.ValidateResponse(req.Context(), &openapi3filter.ResponseValidationInput{
		RequestValidationInput: input,
		Status:                 resp.StatusCode,
		Header:                 resp.Header,
		Body:                   io.NopCloser(bytes.NewReader(respBody)),
		Options:                &openapi3filter.Options{IncludeResponseStatus: true, MultiError: true},
	})
	if err != nil {
		t.tb.Errorf("OpenAPI: response for %s %s (status %d) violates the spec: %v\nbody: %s",
			req.Method, req.URL.Path, resp.StatusCode, err, respBody)
	}
	return resp, nil
}
