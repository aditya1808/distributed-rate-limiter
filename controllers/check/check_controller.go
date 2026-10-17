package check

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/controllers"
	checkschema "github.com/aditya1808/distributed-rate-limiter/schemas/check"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

type Evaluator interface {
	Evaluate(ctx context.Context, tenant, namespace string, cost int64) (types.Decision, error)
}

type Controller struct {
	eval Evaluator
}

func NewController(eval Evaluator) *Controller { return &Controller{eval: eval} }

// Check handles POST /v1/check. Allowed and denied are both 200: a denial is
// a valid decision, and translating it to 429 is the caller's job.
func (ctl *Controller) Check(c *gin.Context) {
	var body checkschema.CheckRequestBody
	if err := c.ShouldBindJSON(&body); err != nil {
		controllers.BindError(c, err)
		return
	}
	if body.Cost == 0 {
		body.Cost = 1
	}
	c.Set(constants.ContextKeyTenant, body.Tenant)

	d, err := ctl.eval.Evaluate(c.Request.Context(), body.Tenant, body.Namespace, body.Cost)
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	c.JSON(http.StatusOK, checkschema.FromDecision(d))
}
