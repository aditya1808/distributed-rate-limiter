package dashboard

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/controllers"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
	dashboardschema "github.com/aditya1808/distributed-rate-limiter/schemas/dashboard"
	quotaschema "github.com/aditya1808/distributed-rate-limiter/schemas/quota"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

//go:embed dashboard.html
var pageSource string

var pageTemplate = template.Must(template.New("dashboard").Parse(pageSource))

const (
	defaultActiveLimit = 25
	pageRows           = 50
	refreshSeconds     = 2
)

type ActivityReader interface {
	Recent(ctx context.Context, limit int) ([]persistence.ActivityEntry, error)
}

type Peeker interface {
	Peek(ctx context.Context, tenant, namespace string) (types.Quota, error)
}

type Controller struct {
	activity   ActivityReader
	peeker     Peeker
	instanceID string
	now        func() time.Time
}

func NewController(a ActivityReader, p Peeker, instanceID string) *Controller {
	return &Controller{activity: a, peeker: p, instanceID: instanceID, now: time.Now}
}

// Page handles GET /dashboard: a server-rendered, read-only view that needs
// no credentials and no JavaScript.
func (ctl *Controller) Page(c *gin.Context) {
	view := pageView{Instance: ctl.instanceID, RefreshSeconds: refreshSeconds, UpdatedAt: ctl.now().UTC().Format("15:04:05 UTC")}
	status := http.StatusOK
	items, err := ctl.collect(c.Request.Context(), pageRows)
	if err != nil {
		status = http.StatusServiceUnavailable
		view.Error = "retrying"
		slog.Warn("[DASHBOARD] activity unavailable", "error", err)
	}
	now := ctl.now()
	for _, it := range items {
		view.Rows = append(view.Rows, newRow(it, now))
	}

	var buf bytes.Buffer
	if err := pageTemplate.Execute(&buf, view); err != nil {
		controllers.HandleError(c, fmt.Errorf("render dashboard: %w", err))
		return
	}
	c.Header("Cache-Control", "no-store")
	c.Header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'")
	c.Data(status, "text/html; charset=utf-8", buf.Bytes())
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
	items, err := ctl.collect(c.Request.Context(), q.Limit)
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	c.JSON(http.StatusOK, dashboardschema.ActiveTenantsResponseBody{Items: items, Count: len(items)})
}

func (ctl *Controller) collect(ctx context.Context, limit int) ([]dashboardschema.ActiveTenant, error) {
	entries, err := ctl.activity.Recent(ctx, limit)
	if err != nil {
		return nil, errors.Join(types.ErrStoreUnavailable, err)
	}
	items := make([]dashboardschema.ActiveTenant, 0, len(entries))
	for _, e := range entries {
		item := dashboardschema.ActiveTenant{Tenant: e.Tenant, Namespace: e.Namespace, LastSeen: e.LastSeen}
		if quota, err := ctl.peeker.Peek(ctx, e.Tenant, e.Namespace); err == nil {
			body := quotaschema.FromQuota(quota)
			item.Quota = &body
		}
		items = append(items, item)
	}
	return items, nil
}

type pageView struct {
	Instance       string
	RefreshSeconds int
	UpdatedAt      string
	Error          string
	Rows           []row
}

type row struct {
	Tenant, Namespace    string
	HasRule              bool
	Algorithm            string
	Used, Limit, Percent int64
	Level                string
	Resets               string
	Adaptive             string
	Adapted              bool
	LastSeen             string
}

func newRow(it dashboardschema.ActiveTenant, now time.Time) row {
	r := row{Tenant: it.Tenant, Namespace: it.Namespace, LastSeen: ago(now.Sub(it.LastSeen))}
	q := it.Quota
	if q == nil {
		return r
	}
	r.HasRule = true
	r.Algorithm = q.Rule.Algorithm
	r.Used, r.Limit = q.Used, q.Limit
	if q.Limit > 0 {
		r.Percent = min(100, q.Used*100/q.Limit)
	}
	switch {
	case r.Percent >= 90:
		r.Level = "bad"
	case r.Percent >= 70:
		r.Level = "warn"
	default:
		r.Level = "ok"
	}
	if d := q.ResetAt.Sub(now); d > time.Second {
		r.Resets = "in " + d.Round(time.Second).String()
	} else {
		r.Resets = "now"
	}
	r.Adaptive = "-"
	if q.AdaptiveMultiplier != 1 {
		r.Adapted = true
		r.Adaptive = "x" + strconv.FormatFloat(q.AdaptiveMultiplier, 'f', -1, 64)
	}
	return r
}

func ago(d time.Duration) string {
	if d < time.Minute {
		return strconv.Itoa(max(0, int(d.Seconds()))) + "s ago"
	}
	return strconv.Itoa(int(d.Minutes())) + "m ago"
}
