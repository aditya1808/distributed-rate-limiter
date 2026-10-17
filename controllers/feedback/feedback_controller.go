package feedback

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/controllers"
	"github.com/aditya1808/distributed-rate-limiter/manager"
	feedbackschema "github.com/aditya1808/distributed-rate-limiter/schemas/feedback"
)

type Applier interface {
	Apply(ctx context.Context, tenant, namespace string, errorRate float64, ttl time.Duration) (manager.AdaptiveResult, error)
}

type Controller struct {
	applier Applier
}

func NewController(a Applier) *Controller { return &Controller{applier: a} }

// Submit handles POST /v1/feedback: a downstream error rate temporarily
// tightens the tenant's limits.
func (ctl *Controller) Submit(c *gin.Context) {
	var body feedbackschema.FeedbackRequestBody
	if err := c.ShouldBindJSON(&body); err != nil {
		controllers.BindError(c, err)
		return
	}
	c.Set(constants.ContextKeyTenant, body.Tenant)

	ttl := time.Duration(body.TTLSeconds) * time.Second
	res, err := ctl.applier.Apply(c.Request.Context(), body.Tenant, body.Namespace, *body.ErrorRate, ttl)
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	c.JSON(http.StatusOK, feedbackschema.FeedbackResponseBody{
		Tenant:             body.Tenant,
		Namespace:          body.Namespace,
		AdaptiveMultiplier: res.Multiplier,
		ExpiresAt:          res.ExpiresAt,
	})
}
