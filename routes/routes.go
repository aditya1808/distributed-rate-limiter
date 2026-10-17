package routes

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/controllers/assignment"
	"github.com/aditya1808/distributed-rate-limiter/controllers/check"
	"github.com/aditya1808/distributed-rate-limiter/controllers/dashboard"
	"github.com/aditya1808/distributed-rate-limiter/controllers/feedback"
	"github.com/aditya1808/distributed-rate-limiter/controllers/health"
	"github.com/aditya1808/distributed-rate-limiter/controllers/quota"
	"github.com/aditya1808/distributed-rate-limiter/controllers/rule"
	"github.com/aditya1808/distributed-rate-limiter/middlewares"
)

type Controllers struct {
	Check      *check.Controller
	Quota      *quota.Controller
	Rule       *rule.Controller
	Assignment *assignment.Controller
	Feedback   *feedback.Controller
	Health     *health.Controller
	Dashboard  *dashboard.Controller
}

// AddRoutes registers the public API. The dashboard is a deliberately open,
// read-only, server-rendered page; every /v1 route requires a JWT.
func AddRoutes(r *gin.Engine, ctl Controllers) {
	r.GET("/healthz", ctl.Health.Liveness)
	r.GET("/readyz", ctl.Health.Readiness)
	r.GET("/dashboard", ctl.Dashboard.Page)

	v1 := r.Group("/v1", middlewares.RequireJWT())
	v1.POST("/check", ctl.Check.Check)
	v1.GET("/quota/:tenant/:namespace", ctl.Quota.Get)
	v1.POST("/rules", ctl.Rule.Create)
	v1.GET("/rules", ctl.Rule.List)
	v1.GET("/rules/:id", ctl.Rule.Get)
	v1.PUT("/rules/:id", ctl.Rule.Update)
	v1.DELETE("/rules/:id", ctl.Rule.Delete)
	v1.GET("/assignments", ctl.Assignment.List)
	v1.PUT("/assignments/:tenant/:namespace", ctl.Assignment.Put)
	v1.GET("/assignments/:tenant/:namespace", ctl.Assignment.Get)
	v1.DELETE("/assignments/:tenant/:namespace", ctl.Assignment.Delete)
	v1.POST("/feedback", ctl.Feedback.Submit)
	v1.GET("/tenants/active", ctl.Dashboard.ActiveTenants)
}

// AddInternalRoutes registers endpoints served only on the internal port,
// which the load balancer never routes to.
func AddInternalRoutes(r *gin.Engine, metricsHandler http.Handler) {
	r.GET("/metrics", gin.WrapH(metricsHandler))
}
