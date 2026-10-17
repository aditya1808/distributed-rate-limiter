package assignment

import (
	"time"

	"github.com/aditya1808/distributed-rate-limiter/types"
)

// AssignmentRequestBody is the full assignment for POST and PUT. Tenant "*"
// is the namespace default.
type AssignmentRequestBody struct {
	Tenant    string `json:"tenant" binding:"required,tenant_selector"`
	Namespace string `json:"namespace" binding:"required,identifier"`
	RuleID    string `json:"rule_id" binding:"required"`
}

func (b AssignmentRequestBody) ToAssignment() types.Assignment {
	return types.Assignment{Tenant: b.Tenant, Namespace: b.Namespace, RuleID: b.RuleID}
}

type AssignmentListQuery struct {
	Tenant    string `form:"tenant" binding:"omitempty,tenant_selector"`
	Namespace string `form:"namespace" binding:"omitempty,identifier"`
	RuleID    string `form:"rule_id"`
}

type AssignmentResponseBody struct {
	ID        string    `json:"id"`
	Tenant    string    `json:"tenant"`
	Namespace string    `json:"namespace"`
	RuleID    string    `json:"rule_id"`
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type AssignmentListResponseBody struct {
	Items []AssignmentResponseBody `json:"items"`
	Count int                      `json:"count"`
}

func FromAssignment(a types.Assignment) AssignmentResponseBody {
	return AssignmentResponseBody{
		ID:        a.ID,
		Tenant:    a.Tenant,
		Namespace: a.Namespace,
		RuleID:    a.RuleID,
		Version:   a.Version,
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
