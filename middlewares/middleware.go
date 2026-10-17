package middlewares

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/controllers"
	"github.com/aditya1808/distributed-rate-limiter/metrics"
)

const maxBodyBytes = 64 << 10

type Options struct {
	Logger           *slog.Logger
	Metrics          *metrics.Metrics
	InstanceID       string
	ExposeInstanceID bool
}

// InitializeMiddlewares installs the global chain on the public router. Order
// matters: recovery wraps everything, and the request id exists before
// anything logs.
func InitializeMiddlewares(r *gin.Engine, o Options) {
	r.HandleMethodNotAllowed = true
	r.Use(
		Recovery(o.Logger),
		RequestID(),
		InstanceID(o.InstanceID, o.ExposeInstanceID),
		BodyLimit(maxBodyBytes),
		AccessLog(o.Logger),
		Metrics(o.Metrics),
	)
	r.NoRoute(func(c *gin.Context) {
		controllers.WriteProblem(c, http.StatusNotFound, "not-found", "Route not found", "", nil)
	})
	r.NoMethod(func(c *gin.Context) {
		controllers.WriteProblem(c, http.StatusMethodNotAllowed, "method-not-allowed", "Method not allowed", "", nil)
	})
}
