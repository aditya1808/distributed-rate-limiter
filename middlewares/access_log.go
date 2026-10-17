package middlewares

import (
	"log/slog"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/constants"
)

func AccessLog(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		route := routeOf(c)
		status := c.Writer.Status()
		level := slog.LevelInfo
		switch {
		case status >= 500:
			level = slog.LevelError
		case strings.HasSuffix(route, "z"): // /healthz, /readyz: probe noise
			level = slog.LevelDebug
		}
		log.Log(c.Request.Context(), level, "[HTTP] request",
			"method", c.Request.Method,
			"route", route,
			"path", c.Request.URL.Path,
			"status", status,
			"latency_ms", float64(time.Since(start).Microseconds())/1000,
			"request_id", c.GetString(constants.ContextKeyRequestID),
			"tenant", c.GetString(constants.ContextKeyTenant),
			"client_ip", c.ClientIP(),
		)
	}
}

// routeOf returns the route template, never the raw path, so logs and metric
// labels stay low-cardinality.
func routeOf(c *gin.Context) string {
	if r := c.FullPath(); r != "" {
		return r
	}
	return "unmatched"
}
