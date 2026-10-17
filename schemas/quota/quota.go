package quota

import (
	"time"

	"github.com/aditya1808/distributed-rate-limiter/schemas/rule"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

type QuotaURI struct {
	Tenant    string `uri:"tenant" binding:"required,identifier"`
	Namespace string `uri:"namespace" binding:"required,identifier"`
}

type QuotaRule struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Algorithm   string `json:"algorithm"`
	Limit       int64  `json:"limit"`
	Period      string `json:"period"`
	Burst       int64  `json:"burst"`
	BurstWindow string `json:"burst_window,omitempty"`
	Version     int64  `json:"version"`
	// Matched is "tenant" for a tenant-specific rule, "default" for the
	// namespace-wide "*" rule.
	Matched string `json:"matched"`
}

type QuotaResponseBody struct {
	Tenant             string    `json:"tenant"`
	Namespace          string    `json:"namespace"`
	Rule               QuotaRule `json:"rule"`
	Limit              int64     `json:"limit"`
	Used               int64     `json:"used"`
	Remaining          int64     `json:"remaining"`
	ResetAt            time.Time `json:"reset_at"`
	AdaptiveMultiplier float64   `json:"adaptive_multiplier"`
}

func FromQuota(q types.Quota) QuotaResponseBody {
	qr := QuotaRule{
		ID:        q.Rule.ID,
		Name:      q.Rule.Name,
		Algorithm: string(q.Rule.Algorithm),
		Limit:     q.Rule.Limit,
		Period:    rule.FormatDuration(q.Rule.Period),
		Burst:     q.Rule.Burst,
		Version:   q.Rule.Version,
		Matched:   string(q.Match),
	}
	if q.Rule.BurstWindow > 0 {
		qr.BurstWindow = rule.FormatDuration(q.Rule.BurstWindow)
	}
	return QuotaResponseBody{
		Tenant:             q.Tenant,
		Namespace:          q.Namespace,
		Rule:               qr,
		Limit:              q.Limit,
		Used:               q.Used,
		Remaining:          q.Remaining,
		ResetAt:            q.ResetAt.UTC(),
		AdaptiveMultiplier: q.Multiplier,
	}
}
