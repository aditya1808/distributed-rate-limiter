package rule

import (
	"context"
	"net/http"
	"strconv"
	"strings"

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
	setETag(c, created.Version)
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
	setETag(c, rule.Version)
	c.JSON(http.StatusOK, ruleschema.FromRule(rule))
}

// Update requires If-Match with the current version (optimistic concurrency),
// so two admins cannot silently overwrite each other.
func (ctl *Controller) Update(c *gin.Context) {
	ifMatch := c.GetHeader("If-Match")
	if ifMatch == "" {
		controllers.WriteProblem(c, http.StatusPreconditionRequired, "precondition-required", "If-Match header required",
			`send If-Match with the rule's current ETag, e.g. If-Match: "3"`, nil)
		return
	}
	expected, ok := parseETag(ifMatch)
	if !ok {
		controllers.Validation(c, []types.FieldError{{Field: "If-Match", Message: `must be an ETag such as "3"`}})
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
	setETag(c, updated.Version)
	c.JSON(http.StatusOK, ruleschema.FromRule(updated))
}

func (ctl *Controller) Delete(c *gin.Context) {
	if err := ctl.mgr.DeleteRule(c.Request.Context(), c.Param("id")); err != nil {
		controllers.HandleError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func setETag(c *gin.Context, version int64) {
	c.Header("ETag", strconv.Quote(strconv.FormatInt(version, 10)))
}

func parseETag(v string) (int64, bool) {
	v = strings.TrimPrefix(strings.TrimSpace(v), "W/")
	v = strings.Trim(v, `"`)
	n, err := strconv.ParseInt(v, 10, 64)
	return n, err == nil && n > 0
}
