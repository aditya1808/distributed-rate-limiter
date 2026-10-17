package assignment

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/controllers"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
	assignmentschema "github.com/aditya1808/distributed-rate-limiter/schemas/assignment"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

type Manager interface {
	CreateAssignment(ctx context.Context, a types.Assignment) (types.Assignment, error)
	UpdateAssignment(ctx context.Context, id string, a types.Assignment, expectedVersion int64) (types.Assignment, error)
	DeleteAssignment(ctx context.Context, id string) error
	GetAssignment(ctx context.Context, id string) (types.Assignment, error)
	ListAssignments(ctx context.Context, f persistence.AssignmentFilter) ([]types.Assignment, error)
}

type Controller struct {
	mgr Manager
}

func NewController(m Manager) *Controller { return &Controller{mgr: m} }

// Create handles POST /v1/assignments. A pair that already has an
// assignment gets 409; the client updates that assignment instead.
func (ctl *Controller) Create(c *gin.Context) {
	var body assignmentschema.AssignmentRequestBody
	if err := c.ShouldBindJSON(&body); err != nil {
		controllers.BindError(c, err)
		return
	}
	created, err := ctl.mgr.CreateAssignment(c.Request.Context(), body.ToAssignment())
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	c.Header("Location", "/v1/assignments/"+created.ID)
	controllers.SetETag(c, created.Version)
	c.JSON(http.StatusCreated, assignmentschema.FromAssignment(created))
}

func (ctl *Controller) List(c *gin.Context) {
	var q assignmentschema.AssignmentListQuery
	if err := c.ShouldBindQuery(&q); err != nil {
		controllers.BindError(c, err)
		return
	}
	as, err := ctl.mgr.ListAssignments(c.Request.Context(), persistence.AssignmentFilter{
		Tenant: q.Tenant, Namespace: q.Namespace, RuleID: q.RuleID,
	})
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	c.JSON(http.StatusOK, assignmentschema.FromAssignments(as))
}

func (ctl *Controller) Get(c *gin.Context) {
	a, err := ctl.mgr.GetAssignment(c.Request.Context(), c.Param("id"))
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	controllers.SetETag(c, a.Version)
	c.JSON(http.StatusOK, assignmentschema.FromAssignment(a))
}

// Update replaces the assignment and requires If-Match with its current
// version.
func (ctl *Controller) Update(c *gin.Context) {
	expected, ok := controllers.IfMatch(c)
	if !ok {
		return
	}
	var body assignmentschema.AssignmentRequestBody
	if err := c.ShouldBindJSON(&body); err != nil {
		controllers.BindError(c, err)
		return
	}
	updated, err := ctl.mgr.UpdateAssignment(c.Request.Context(), c.Param("id"), body.ToAssignment(), expected)
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	controllers.SetETag(c, updated.Version)
	c.JSON(http.StatusOK, assignmentschema.FromAssignment(updated))
}

func (ctl *Controller) Delete(c *gin.Context) {
	if err := ctl.mgr.DeleteAssignment(c.Request.Context(), c.Param("id")); err != nil {
		controllers.HandleError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
