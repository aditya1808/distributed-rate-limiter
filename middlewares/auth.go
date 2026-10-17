package middlewares

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/controllers"
)

// AdminAuth protects admin routes. It accepts "Authorization: Bearer <token>"
// for API clients and HTTP Basic (any username, token as password) so the
// dashboard can use the browser's native login prompt.
func AdminAuth(token string) gin.HandlerFunc {
	want := []byte(token)
	return func(c *gin.Context) {
		if got, ok := presentedToken(c.Request); ok && subtle.ConstantTimeCompare([]byte(got), want) == 1 {
			c.Next()
			return
		}
		c.Header("WWW-Authenticate", `Bearer realm="ratelimiter", Basic realm="ratelimiter", charset="UTF-8"`)
		controllers.WriteProblem(c, http.StatusUnauthorized, "unauthorized", "Authentication required",
			"provide the admin token as a Bearer token or Basic auth password", nil)
	}
}

func presentedToken(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if t, ok := strings.CutPrefix(header, "Bearer "); ok {
		return strings.TrimSpace(t), true
	}
	if _, password, ok := r.BasicAuth(); ok {
		return password, true
	}
	return "", false
}
