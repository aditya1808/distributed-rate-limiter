package middlewares

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/controllers"
)

const maxJWTLength = 8 << 10

// RequireJWT requires "Authorization: Bearer <jwt>" with a well-formed JWT:
// three base64url segments, a JSON header naming a signing algorithm other
// than "none", a JSON object payload, and a non-empty signature.
//
// TODO(auth): this is a format check only, NOT authentication. The signature,
// issuer, audience and expiry are not verified, so any well-formed token is
// accepted on every /v1 route, including rule management. Before production:
//   - authn: verify the signature against the identity provider's JWKS
//     (RS256/ES256 only), and check iss, aud, exp and nbf;
//   - authz: require scopes per route (e.g. ratelimit:check for decisions,
//     ratelimit:admin for rules, feedback and the dashboard) and a tenants
//     claim so a caller can only consume its own tenants' quota;
//   - audit-log rule changes with the caller identity (sub).
func RequireJWT() gin.HandlerFunc {
	return func(c *gin.Context) {
		token, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer ")
		if !ok || strings.TrimSpace(token) == "" {
			c.Header("WWW-Authenticate", `Bearer realm="ratelimiter"`)
			controllers.WriteProblem(c, http.StatusUnauthorized, "unauthorized", "Authentication required",
				"provide a JWT as a Bearer token", nil)
			return
		}
		if !wellFormedJWT(strings.TrimSpace(token)) {
			c.Header("WWW-Authenticate", `Bearer realm="ratelimiter", error="invalid_token"`)
			controllers.WriteProblem(c, http.StatusUnauthorized, "invalid-token", "Invalid token",
				"the Bearer token is not a well-formed JWT", nil)
			return
		}
		c.Next()
	}
}

func wellFormedJWT(token string) bool {
	if len(token) > maxJWTLength {
		return false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[2] == "" {
		return false
	}
	var header struct {
		Alg string `json:"alg"`
	}
	if !decodeSegment(parts[0], &header) || header.Alg == "" || strings.EqualFold(header.Alg, "none") {
		return false
	}
	var payload map[string]any
	if !decodeSegment(parts[1], &payload) || payload == nil {
		return false
	}
	_, err := base64.RawURLEncoding.DecodeString(parts[2])
	return err == nil
}

func decodeSegment(segment string, v any) bool {
	data, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		return false
	}
	return json.Unmarshal(data, v) == nil
}
