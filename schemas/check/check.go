package check

import (
	"time"

	"github.com/aditya1808/distributed-rate-limiter/types"
)

type CheckRequestBody struct {
	Tenant    string `json:"tenant" binding:"required,identifier"`
	Namespace string `json:"namespace" binding:"required,identifier"`
	// Cost defaults to 1.
	Cost int64 `json:"cost" binding:"omitempty,min=1,max=1000"`
}

type CheckResponseBody struct {
	Allowed            bool       `json:"allowed"`
	Tenant             string     `json:"tenant"`
	Namespace          string     `json:"namespace"`
	RuleID             string     `json:"rule_id,omitempty"`
	Algorithm          string     `json:"algorithm,omitempty"`
	Limit              int64      `json:"limit"`
	Remaining          int64      `json:"remaining"`
	ResetAt            *time.Time `json:"reset_at,omitempty"`
	RetryAfterMs       int64      `json:"retry_after_ms"`
	Degraded           bool       `json:"degraded"`
	Reason             string     `json:"reason,omitempty"`
	AdaptiveMultiplier float64    `json:"adaptive_multiplier"`
}

func FromDecision(d types.Decision) CheckResponseBody {
	body := CheckResponseBody{
		Allowed:            d.Allowed,
		Tenant:             d.Tenant,
		Namespace:          d.Namespace,
		RuleID:             d.RuleID,
		Algorithm:          string(d.Algorithm),
		Limit:              d.Limit,
		Remaining:          d.Remaining,
		RetryAfterMs:       d.RetryAfter.Milliseconds(),
		Degraded:           d.Degraded,
		Reason:             d.Reason,
		AdaptiveMultiplier: d.Multiplier,
	}
	if !d.ResetAt.IsZero() {
		t := d.ResetAt.UTC()
		body.ResetAt = &t
	}
	return body
}
