package rule

import (
	"strconv"
	"time"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

type ruleFields struct {
	Namespace   string `json:"namespace" binding:"required,identifier"`
	Algorithm   string `json:"algorithm" binding:"required,oneof=token_bucket sliding_window"`
	Limit       int64  `json:"limit" binding:"required,min=1,max=1000000"`
	Period      string `json:"period" binding:"required"`
	Burst       int64  `json:"burst" binding:"omitempty,min=0,max=1000000"`
	BurstWindow string `json:"burst_window"`
	FailOpen    bool   `json:"fail_open"`
}

type RulePostRequestBody struct {
	// Tenant defaults to "*", the namespace-wide default rule.
	Tenant string `json:"tenant" binding:"omitempty,tenant_selector"`
	ruleFields
}

type RulePutRequestBody struct {
	// Tenant is required on update so that scope changes are explicit.
	Tenant string `json:"tenant" binding:"required,tenant_selector"`
	ruleFields
}

func (b RulePostRequestBody) ToRule() (types.Rule, error) {
	tenant := b.Tenant
	if tenant == "" {
		tenant = constants.DefaultTenant
	}
	return b.toRule(tenant)
}

func (b RulePutRequestBody) ToRule() (types.Rule, error) {
	return b.toRule(b.Tenant)
}

func (f ruleFields) toRule(tenant string) (types.Rule, error) {
	var errs []types.FieldError
	period, err := time.ParseDuration(f.Period)
	if err != nil {
		errs = append(errs, types.FieldError{Field: "period", Message: `must be a duration such as "1s", "1m" or "1h"`})
	}
	var burstWindow time.Duration
	if f.BurstWindow != "" {
		burstWindow, err = time.ParseDuration(f.BurstWindow)
		if err != nil {
			errs = append(errs, types.FieldError{Field: "burst_window", Message: `must be a duration such as "1s"`})
		}
	}
	if len(errs) > 0 {
		return types.Rule{}, types.NewValidationError(errs...)
	}
	return types.Rule{
		Tenant:      tenant,
		Namespace:   f.Namespace,
		Algorithm:   types.Algorithm(f.Algorithm),
		Limit:       f.Limit,
		Period:      period,
		Burst:       f.Burst,
		BurstWindow: burstWindow,
		FailOpen:    f.FailOpen,
	}, nil
}

type RuleResponseBody struct {
	ID          string    `json:"id"`
	Tenant      string    `json:"tenant"`
	Namespace   string    `json:"namespace"`
	Algorithm   string    `json:"algorithm"`
	Limit       int64     `json:"limit"`
	Period      string    `json:"period"`
	Burst       int64     `json:"burst"`
	BurstWindow string    `json:"burst_window,omitempty"`
	FailOpen    bool      `json:"fail_open"`
	Version     int64     `json:"version"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type RuleListResponseBody struct {
	Items []RuleResponseBody `json:"items"`
	Count int                `json:"count"`
}

func FromRule(r types.Rule) RuleResponseBody {
	body := RuleResponseBody{
		ID:        r.ID,
		Tenant:    r.Tenant,
		Namespace: r.Namespace,
		Algorithm: string(r.Algorithm),
		Limit:     r.Limit,
		Period:    FormatDuration(r.Period),
		Burst:     r.Burst,
		FailOpen:  r.FailOpen,
		Version:   r.Version,
		CreatedAt: r.CreatedAt.UTC(),
		UpdatedAt: r.UpdatedAt.UTC(),
	}
	if r.BurstWindow > 0 {
		body.BurstWindow = FormatDuration(r.BurstWindow)
	}
	return body
}

func FromRules(rules []types.Rule) RuleListResponseBody {
	items := make([]RuleResponseBody, 0, len(rules))
	for _, r := range rules {
		items = append(items, FromRule(r))
	}
	return RuleListResponseBody{Items: items, Count: len(items)}
}

// FormatDuration renders durations the way users write them ("1m", not "1m0s").
func FormatDuration(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return strconv.FormatInt(int64(d/time.Hour), 10) + "h"
	case d%time.Minute == 0:
		return strconv.FormatInt(int64(d/time.Minute), 10) + "m"
	case d%time.Second == 0:
		return strconv.FormatInt(int64(d/time.Second), 10) + "s"
	default:
		return strconv.FormatInt(d.Milliseconds(), 10) + "ms"
	}
}
