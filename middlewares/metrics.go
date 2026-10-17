package middlewares

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/metrics"
)

func Metrics(m *metrics.Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		route := routeOf(c)
		m.HTTPRequests.WithLabelValues(route, c.Request.Method, strconv.Itoa(c.Writer.Status())).Inc()
		m.HTTPDuration.WithLabelValues(route, c.Request.Method).Observe(time.Since(start).Seconds())
	}
}
