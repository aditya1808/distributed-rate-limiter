package persistence

import (
	"context"
	"time"

	"github.com/aditya1808/distributed-rate-limiter/types"
)

// RuleStore is the source of truth for rules and their assignments, shared
// by every instance.
type RuleStore interface {
	CreateRule(ctx context.Context, rule types.Rule) error
	// UpdateRule replaces the rule only if the stored version equals
	// expectedVersion, otherwise types.ErrVersionMismatch.
	UpdateRule(ctx context.Context, rule types.Rule, expectedVersion int64) error
	// DeleteRule fails with types.ErrRuleInUse while assignments reference it.
	DeleteRule(ctx context.Context, id string) error
	GetRule(ctx context.Context, id string) (types.Rule, error)
	ListRules(ctx context.Context) ([]types.Rule, error)

	// PutAssignment creates or replaces the assignment for its pair and
	// reports whether it was created. types.ErrUnknownRule if the rule is missing.
	PutAssignment(ctx context.Context, a types.Assignment) (types.Assignment, bool, error)
	DeleteAssignment(ctx context.Context, tenant, namespace string) error
	GetAssignment(ctx context.Context, tenant, namespace string) (types.Assignment, error)
	ListAssignments(ctx context.Context, f AssignmentFilter) ([]types.Assignment, error)

	// Bindings returns every assignment joined with its rule.
	Bindings(ctx context.Context) ([]types.Binding, error)
	// Resolve returns the effective rule for one pair.
	Resolve(ctx context.Context, tenant, namespace string) (types.Resolution, error)
}

type AssignmentFilter struct {
	Tenant    string
	Namespace string
	RuleID    string
}

// ResolutionCache is a short-lived, cross-instance cache of Resolve results
// in front of the RuleStore, consulted only when an instance's in-memory
// snapshot has no rule for a pair.
type ResolutionCache interface {
	GetResolution(ctx context.Context, tenant, namespace string) (types.Resolution, bool, error)
	SetResolution(ctx context.Context, tenant, namespace string, res types.Resolution, ttl time.Duration) error
	DeleteResolution(ctx context.Context, tenant, namespace string) error
}

// ChangeEvent is broadcast to every instance after a rule or assignment
// write. It carries no data: receivers reload from the RuleStore.
type ChangeEvent struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

const (
	ChangeRule       = "rule"
	ChangeAssignment = "assignment"
)

type ChangeBus interface {
	Publish(ctx context.Context, ev ChangeEvent) error
	// Subscribe returns once the subscription is active. The channel closes
	// when ctx is cancelled.
	Subscribe(ctx context.Context) (<-chan ChangeEvent, error)
}

type AdaptiveStore interface {
	SetMultiplier(ctx context.Context, tenant, namespace string, mult float64, ttl time.Duration) error
	ClearMultiplier(ctx context.Context, tenant, namespace string) error
}

type ActivityEntry struct {
	Tenant    string
	Namespace string
	LastSeen  time.Time
}

type ActivityStore interface {
	// Record upserts last-seen timestamps keyed by constants.PairKey.
	Record(ctx context.Context, seen map[string]time.Time) error
	Recent(ctx context.Context, limit int) ([]ActivityEntry, error)
	Trim(ctx context.Context, olderThan time.Time) error
}
