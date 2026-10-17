package types

import "time"

type MatchKind string

const (
	MatchTenant  MatchKind = "tenant"
	MatchDefault MatchKind = "default"
)

const (
	ReasonQuotaExceeded       = "quota_exceeded"
	ReasonNoRule              = "no_rule"
	ReasonCostExceedsCapacity = "cost_exceeds_capacity"
	ReasonStoreUnavailable    = "store_unavailable"
)

// Decision is the outcome of a single quota evaluation.
type Decision struct {
	Allowed    bool
	Tenant     string
	Namespace  string
	RuleID     string
	Algorithm  Algorithm
	Limit      int64
	Remaining  int64
	ResetAt    time.Time
	RetryAfter time.Duration
	Degraded   bool
	Reason     string
	Multiplier float64
}

// Quota is a read-only view of the current state for a (tenant, namespace).
type Quota struct {
	Tenant     string
	Namespace  string
	Rule       Rule
	Match      MatchKind
	Limit      int64
	Used       int64
	Remaining  int64
	ResetAt    time.Time
	Multiplier float64
}
