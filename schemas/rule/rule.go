package rule

import (
	"strconv"
	"time"

	"github.com/aditya1808/distributed-rate-limiter/types"
)

type ruleFields struct {
	Algorithm   string `json:"algorithm" binding:"required,oneof=token_bucket sliding_window"`
	Limit       int64  `json:"limit" binding:"required,min=1,max=1000000"`
	Period      string `json:"period" binding:"required"`
	Burst       int64  `json:"burst" binding:"omitempty,min=0,max=1000000"`
	BurstWindow string `json:"burst_window"`
	FailOpen    bool   `json:"fail_open"`
}

// RulePostRequestBody and RulePutRequestBody are the same shape: a rule has no
// scope of its own; assignments bind it to (tenant, namespace) pairs.
type RulePostRequestBody struct{ ruleFields }

type RulePutRequestBody struct{ ruleFields }

func (b RulePostRequestBody) ToRule() (types.Rule, error) { return b.toRule() }

func (b RulePutRequestBody) ToRule() (types.Rule, error) { return b.toRule() }

func (f ruleFields) toRule() (types.Rule, error) {
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
