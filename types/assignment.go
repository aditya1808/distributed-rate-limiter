package types

import (
	"time"

	"github.com/aditya1808/distributed-rate-limiter/constants"
)

// Assignment binds a rule to a (tenant, namespace) pair. Tenant "*" makes the
// rule the default for every tenant in the namespace without its own
// assignment. A pair has at most one assignment; a rule can serve many pairs.
type Assignment struct {
	Tenant    string    `json:"tenant"`
	Namespace string    `json:"namespace"`
	RuleID    string    `json:"rule_id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (a Assignment) PairKey() string { return constants.PairKey(a.Tenant, a.Namespace) }

func (a Assignment) Validate() error {
	var errs []FieldError
	if a.Tenant != constants.DefaultTenant && !IdentifierPattern.MatchString(a.Tenant) {
		errs = append(errs, FieldError{Field: "tenant", Message: "must be \"*\" or match " + IdentifierPattern.String()})
	}
	if !IdentifierPattern.MatchString(a.Namespace) {
		errs = append(errs, FieldError{Field: "namespace", Message: "must match " + IdentifierPattern.String()})
	}
	if a.RuleID == "" {
		errs = append(errs, FieldError{Field: "rule_id", Message: "is required"})
	}
	if len(errs) > 0 {
		return NewValidationError(errs...)
	}
	return nil
}

// Binding is an assignment joined with its rule: what the rule cache holds.
type Binding struct {
	Tenant    string
	Namespace string
	Rule      Rule
}

func (b Binding) PairKey() string { return constants.PairKey(b.Tenant, b.Namespace) }

// Resolution is the effective rule for one (tenant, namespace): the tenant's
// own assignment, else the namespace default, else none (Found false).
type Resolution struct {
	Found bool      `json:"found"`
	Match MatchKind `json:"match,omitempty"`
	Rule  Rule      `json:"rule"`
}
