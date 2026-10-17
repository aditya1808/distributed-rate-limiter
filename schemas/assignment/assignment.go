package assignment

import (
	"time"

	"github.com/aditya1808/distributed-rate-limiter/types"
)

type AssignmentURI struct {
	// Tenant "*" is the namespace default.
	Tenant    string `uri:"tenant" binding:"required,tenant_selector"`
	Namespace string `uri:"namespace" binding:"required,identifier"`
}

type AssignmentPutRequestBody struct {
	RuleID string `json:"rule_id" binding:"required"`
}

type AssignmentListQuery struct {
	Tenant    string `form:"tenant" binding:"omitempty,tenant_selector"`
	Namespace string `form:"namespace" binding:"omitempty,identifier"`
	RuleID    string `form:"rule_id"`
}

type AssignmentResponseBody struct {
	Tenant    string    `json:"tenant"`
	Namespace string    `json:"namespace"`
	RuleID    string    `json:"rule_id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type AssignmentListResponseBody struct {
	Items []AssignmentResponseBody `json:"items"`
	Count int                      `json:"count"`
}

func FromAssignment(a types.Assignment) AssignmentResponseBody {
	return AssignmentResponseBody{
		Tenant:    a.Tenant,
		Namespace: a.Namespace,
		RuleID:    a.RuleID,
		CreatedAt: a.CreatedAt.UTC(),
		UpdatedAt: a.UpdatedAt.UTC(),
	}
}

func FromAssignments(as []types.Assignment) AssignmentListResponseBody {
	items := make([]AssignmentResponseBody, 0, len(as))
	for _, a := range as {
		items = append(items, FromAssignment(a))
	}
	return AssignmentListResponseBody{Items: items, Count: len(items)}
}
