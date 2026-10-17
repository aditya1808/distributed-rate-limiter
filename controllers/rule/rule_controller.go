package rule

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/controllers"
	ruleschema "github.com/aditya1808/distributed-rate-limiter/schemas/rule"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

type Manager interface {
	CreateRule(ctx context.Context, rule types.Rule) (types.Rule, error)
	UpdateRule(ctx context.Context, id string, rule types.Rule, expectedVersion int64) (types.Rule, error)
	DeleteRule(ctx context.Context, id string) error
	GetRule(ctx context.Context, id string) (types.Rule, error)
	ListRules(ctx context.Context) ([]types.Rule, error)
}

type Controller struct {
	mgr Manager
}

func NewController(m Manager) *Controller { return &Controller{mgr: m} }

func (ctl *Controller) Create(c *gin.Context) {
	var body ruleschema.RulePostRequestBody
	if err := c.ShouldBindJSON(&body); err != nil {
		controllers.BindError(c, err)
		return
	}
	rule, err := body.ToRule()
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	created, err := ctl.mgr.CreateRule(c.Request.Context(), rule)
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	c.Header("Location", "/v1/rules/"+created.ID)
	controllers.SetETag(c, created.Version)
	c.JSON(http.StatusCreated, ruleschema.FromRule(created))
}

func (ctl *Controller) List(c *gin.Context) {
	rules, err := ctl.mgr.ListRules(c.Request.Context())
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	c.JSON(http.StatusOK, ruleschema.FromRules(rules))
}

func (ctl *Controller) Get(c *gin.Context) {
	rule, err := ctl.mgr.GetRule(c.Request.Context(), c.Param("id"))
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	controllers.SetETag(c, rule.Version)
	c.JSON(http.StatusOK, ruleschema.FromRule(rule))
}

// Update requires If-Match with the current version (optimistic concurrency),
// so two admins cannot silently overwrite each other.
func (ctl *Controller) Update(c *gin.Context) {
	expected, ok := controllers.IfMatch(c)
	if !ok {
		return
	}

	var body ruleschema.RulePutRequestBody
	if err := c.ShouldBindJSON(&body); err != nil {
		controllers.BindError(c, err)
		return
	}
	rule, err := body.ToRule()
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	updated, err := ctl.mgr.UpdateRule(c.Request.Context(), c.Param("id"), rule, expected)
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	controllers.SetETag(c, updated.Version)
	c.JSON(http.StatusOK, ruleschema.FromRule(updated))
}

func (ctl *Controller) Delete(c *gin.Context) {
	if err := ctl.mgr.DeleteRule(c.Request.Context(), c.Param("id")); err != nil {
		controllers.HandleError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
