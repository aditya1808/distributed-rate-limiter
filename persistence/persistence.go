package persistence

import (
	"context"
	"time"

	"github.com/aditya1808/distributed-rate-limiter/types"
)

// RuleStore is the source of truth for rules, shared by every instance.
type RuleStore interface {
	// Create fails with types.ErrConflict if a rule already exists for the
	// rule's (tenant, namespace).
	Create(ctx context.Context, rule types.Rule) error
	// Update replaces the rule only if the stored version equals
	// expectedVersion, otherwise types.ErrVersionMismatch.
	Update(ctx context.Context, rule types.Rule, expectedVersion int64) error
	Delete(ctx context.Context, id string) error
	Get(ctx context.Context, id string) (types.Rule, error)
	List(ctx context.Context) ([]types.Rule, error)
}

type ChangeOp string

const (
	ChangeUpsert ChangeOp = "upsert"
	ChangeDelete ChangeOp = "delete"
)

// ChangeEvent is broadcast to every instance after a rule write.
type ChangeEvent struct {
	Op      ChangeOp `json:"op"`
	ID      string   `json:"id"`
	Version int64    `json:"version"`
}

type ChangeSubscriber interface {
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
