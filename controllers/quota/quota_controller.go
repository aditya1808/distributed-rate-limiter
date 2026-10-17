package quota

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/controllers"
	quotaschema "github.com/aditya1808/distributed-rate-limiter/schemas/quota"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

type Peeker interface {
	Peek(ctx context.Context, tenant, namespace string) (types.Quota, error)
}

type Controller struct {
	peeker Peeker
}

func NewController(p Peeker) *Controller { return &Controller{peeker: p} }

// Get handles GET /v1/quota/:tenant/:namespace without consuming quota.
func (ctl *Controller) Get(c *gin.Context) {
	var uri quotaschema.QuotaURI
	if err := c.ShouldBindUri(&uri); err != nil {
		controllers.BindError(c, err)
		return
	}
	c.Set(constants.ContextKeyTenant, uri.Tenant)

	q, err := ctl.peeker.Peek(c.Request.Context(), uri.Tenant, uri.Namespace)
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	c.JSON(http.StatusOK, quotaschema.FromQuota(q))
}
