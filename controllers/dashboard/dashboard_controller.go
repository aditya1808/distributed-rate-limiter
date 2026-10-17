package dashboard

import (
	"context"
	_ "embed"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/controllers"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
	dashboardschema "github.com/aditya1808/distributed-rate-limiter/schemas/dashboard"
	quotaschema "github.com/aditya1808/distributed-rate-limiter/schemas/quota"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

//go:embed dashboard.html
var page []byte

const defaultActiveLimit = 25

type ActivityReader interface {
	Recent(ctx context.Context, limit int) ([]persistence.ActivityEntry, error)
}

type Peeker interface {
	Peek(ctx context.Context, tenant, namespace string) (types.Quota, error)
}

type Controller struct {
	activity ActivityReader
	peeker   Peeker
}

func NewController(a ActivityReader, p Peeker) *Controller {
	return &Controller{activity: a, peeker: p}
}

func (ctl *Controller) Page(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Header("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'")
	c.Data(http.StatusOK, "text/html; charset=utf-8", page)
}

// ActiveTenants handles GET /v1/tenants/active: recently active pairs with
// their current quota.
func (ctl *Controller) ActiveTenants(c *gin.Context) {
	var q dashboardschema.ActiveTenantsQuery
	if err := c.ShouldBindQuery(&q); err != nil {
		controllers.BindError(c, err)
		return
	}
	if q.Limit == 0 {
		q.Limit = defaultActiveLimit
	}
	entries, err := ctl.activity.Recent(c.Request.Context(), q.Limit)
	if err != nil {
		controllers.HandleError(c, errors.Join(types.ErrStoreUnavailable, err))
		return
	}
	items := make([]dashboardschema.ActiveTenant, 0, len(entries))
	for _, e := range entries {
		item := dashboardschema.ActiveTenant{Tenant: e.Tenant, Namespace: e.Namespace, LastSeen: e.LastSeen}
		if quota, err := ctl.peeker.Peek(c.Request.Context(), e.Tenant, e.Namespace); err == nil {
			body := quotaschema.FromQuota(quota)
			item.Quota = &body
		}
		items = append(items, item)
	}
	c.JSON(http.StatusOK, dashboardschema.ActiveTenantsResponseBody{Items: items, Count: len(items)})
}
