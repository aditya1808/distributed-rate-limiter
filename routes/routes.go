package routes

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/controllers/check"
	"github.com/aditya1808/distributed-rate-limiter/controllers/dashboard"
	"github.com/aditya1808/distributed-rate-limiter/controllers/feedback"
	"github.com/aditya1808/distributed-rate-limiter/controllers/health"
	"github.com/aditya1808/distributed-rate-limiter/controllers/quota"
	"github.com/aditya1808/distributed-rate-limiter/controllers/rule"
	"github.com/aditya1808/distributed-rate-limiter/middlewares"
)

type Controllers struct {
	Check     *check.Controller
	Quota     *quota.Controller
	Rule      *rule.Controller
	Feedback  *feedback.Controller
	Health    *health.Controller
	Dashboard *dashboard.Controller
}

// AddRoutes registers the public API. Decisions require a JWT; everything that
// changes behaviour or reveals other tenants sits behind admin auth.
func AddRoutes(r *gin.Engine, ctl Controllers, adminToken string) {
	admin := middlewares.AdminAuth(adminToken)

	r.GET("/healthz", ctl.Health.Liveness)
	r.GET("/readyz", ctl.Health.Readiness)
	r.GET("/dashboard", admin, ctl.Dashboard.Page)

	v1 := r.Group("/v1")

	callerV1 := v1.Group("", middlewares.RequireJWT())
	callerV1.POST("/check", ctl.Check.Check)
	callerV1.GET("/quota/:tenant/:namespace", ctl.Quota.Get)

	adminV1 := v1.Group("", admin)
	adminV1.POST("/rules", ctl.Rule.Create)
	adminV1.GET("/rules", ctl.Rule.List)
	adminV1.GET("/rules/:id", ctl.Rule.Get)
	adminV1.PUT("/rules/:id", ctl.Rule.Update)
	adminV1.DELETE("/rules/:id", ctl.Rule.Delete)
	adminV1.POST("/feedback", ctl.Feedback.Submit)
	adminV1.GET("/tenants/active", ctl.Dashboard.ActiveTenants)
}

// AddInternalRoutes registers endpoints served only on the internal port,
// which the load balancer never routes to.
func AddInternalRoutes(r *gin.Engine, metricsHandler http.Handler) {
	r.GET("/metrics", gin.WrapH(metricsHandler))
}
