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
	Assign(ctx context.Context, a types.Assignment) (types.Assignment, bool, error)
	Unassign(ctx context.Context, tenant, namespace string) error
	GetAssignment(ctx context.Context, tenant, namespace string) (types.Assignment, error)
	ListAssignments(ctx context.Context, f persistence.AssignmentFilter) ([]types.Assignment, error)
}

type Controller struct {
	mgr Manager
}

func NewController(m Manager) *Controller { return &Controller{mgr: m} }

// Put handles PUT /v1/assignments/:tenant/:namespace. It is idempotent:
// 201 when the pair had no assignment, 200 when one was replaced.
func (ctl *Controller) Put(c *gin.Context) {
	var uri assignmentschema.AssignmentURI
	if err := c.ShouldBindUri(&uri); err != nil {
		controllers.BindError(c, err)
		return
	}
	var body assignmentschema.AssignmentPutRequestBody
	if err := c.ShouldBindJSON(&body); err != nil {
		controllers.BindError(c, err)
		return
	}
	saved, created, err := ctl.mgr.Assign(c.Request.Context(), types.Assignment{
		Tenant: uri.Tenant, Namespace: uri.Namespace, RuleID: body.RuleID,
	})
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	c.JSON(status, assignmentschema.FromAssignment(saved))
}

func (ctl *Controller) Get(c *gin.Context) {
	var uri assignmentschema.AssignmentURI
	if err := c.ShouldBindUri(&uri); err != nil {
		controllers.BindError(c, err)
		return
	}
	a, err := ctl.mgr.GetAssignment(c.Request.Context(), uri.Tenant, uri.Namespace)
	if err != nil {
		controllers.HandleError(c, err)
		return
	}
	c.JSON(http.StatusOK, assignmentschema.FromAssignment(a))
}

func (ctl *Controller) Delete(c *gin.Context) {
	var uri assignmentschema.AssignmentURI
	if err := c.ShouldBindUri(&uri); err != nil {
		controllers.BindError(c, err)
		return
	}
	if err := ctl.mgr.Unassign(c.Request.Context(), uri.Tenant, uri.Namespace); err != nil {
		controllers.HandleError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
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
