package health

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
)

type Controller struct {
	ping         func(ctx context.Context) error
	rulesLoaded  func() bool
	shuttingDown *atomic.Bool
	instanceID   string
}

func NewController(ping func(ctx context.Context) error, rulesLoaded func() bool, shuttingDown *atomic.Bool, instanceID string) *Controller {
	return &Controller{ping: ping, rulesLoaded: rulesLoaded, shuttingDown: shuttingDown, instanceID: instanceID}
}

// Liveness never checks dependencies: a Redis outage must not get healthy
// instances restarted.
func (ctl *Controller) Liveness(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "instance": ctl.instanceID})
}

// Readiness gates load balancer traffic: only instances that can actually
// decide receive requests.
func (ctl *Controller) Readiness(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 200*time.Millisecond)
	defer cancel()

	redisOK := ctl.ping(ctx) == nil
	loaded := ctl.rulesLoaded()
	shutting := ctl.shuttingDown.Load()

	status, code := "ready", http.StatusOK
	if !redisOK || !loaded || shutting {
		status, code = "not_ready", http.StatusServiceUnavailable
	}
	c.JSON(code, gin.H{
		"status":   status,
		"instance": ctl.instanceID,
		"checks": gin.H{
			"redis":         map[bool]string{true: "up", false: "down"}[redisOK],
			"rules_loaded":  loaded,
			"shutting_down": shutting,
		},
	})
}
