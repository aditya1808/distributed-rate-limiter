package middlewares_test

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"github.com/aditya1808/distributed-rate-limiter/middlewares"
	"github.com/aditya1808/distributed-rate-limiter/testutils"
)

func seg(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

func TestRequireJWT(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/", middlewares.RequireJWT(), func(c *gin.Context) { c.Status(http.StatusNoContent) })

	header := seg(`{"alg":"RS256","typ":"JWT"}`)
	payload := seg(`{"sub":"svc"}`)
	sig := seg("signature")

	tests := []struct {
		name   string
		auth   string
		status int
	}{
		{"well-formed", "Bearer " + header + "." + payload + "." + sig, http.StatusNoContent},
		{"test helper token", "Bearer " + testutils.CallerToken, http.StatusNoContent},
		{"missing header", "", http.StatusUnauthorized},
		{"basic scheme", "Basic dXNlcjpwYXNz", http.StatusUnauthorized},
		{"empty bearer", "Bearer ", http.StatusUnauthorized},
		{"opaque token", "Bearer not-a-jwt", http.StatusUnauthorized},
		{"two segments", "Bearer " + header + "." + payload, http.StatusUnauthorized},
		{"four segments", "Bearer " + header + "." + payload + "." + sig + "." + sig, http.StatusUnauthorized},
		{"empty signature", "Bearer " + header + "." + payload + ".", http.StatusUnauthorized},
		{"alg none", "Bearer " + seg(`{"alg":"none"}`) + "." + payload + "." + sig, http.StatusUnauthorized},
		{"alg missing", "Bearer " + seg(`{"typ":"JWT"}`) + "." + payload + "." + sig, http.StatusUnauthorized},
		{"header not json", "Bearer " + seg("nope") + "." + payload + "." + sig, http.StatusUnauthorized},
		{"payload not object", "Bearer " + header + "." + seg(`[1,2]`) + "." + sig, http.StatusUnauthorized},
		{"padded base64", "Bearer " + header + "." + base64.URLEncoding.EncodeToString([]byte(`{"a":1}`)) + "." + sig, http.StatusUnauthorized},
		{"oversized", "Bearer " + header + "." + seg(`{"x":"`+strings.Repeat("a", 9000)+`"}`) + "." + sig, http.StatusUnauthorized},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			assert.Equal(t, tc.status, w.Code)
			if tc.status == http.StatusUnauthorized {
				assert.Contains(t, w.Header().Get("WWW-Authenticate"), "Bearer")
				assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
			}
		})
	}
}
