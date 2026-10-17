package manager

import (
	"context"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/aditya1808/distributed-rate-limiter/persistence"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

type LocalRuleCache interface {
	Upsert(rule types.Rule)
	Remove(id string)
}

type RuleFilter struct {
	Tenant    string
	Namespace string
}

// RuleManager owns rule lifecycle. Writes go to the shared store (which
// broadcasts them to every instance) and are applied to the local cache
// immediately so this instance enforces them without waiting for pub/sub.
// Reads go to the store, not the cache, so admins always read their writes.
type RuleManager struct {
	store persistence.RuleStore
	cache LocalRuleCache
	now   func() time.Time
}

func NewRuleManager(store persistence.RuleStore, cache LocalRuleCache) *RuleManager {
	return &RuleManager{store: store, cache: cache, now: time.Now}
}

func (m *RuleManager) Create(ctx context.Context, rule types.Rule) (types.Rule, error) {
	if err := rule.Validate(); err != nil {
		return types.Rule{}, err
	}
	now := m.now().UTC()
	rule.ID = "r_" + strings.ToLower(ulid.Make().String())
	rule.Version = 1
	rule.CreatedAt = now
	rule.UpdatedAt = now
	if err := m.store.Create(ctx, rule); err != nil {
		return types.Rule{}, err
	}
	m.cache.Upsert(rule)
	return rule, nil
}

// Update replaces a rule if expectedVersion matches. Tenant and namespace
// identify the rule's scope and are immutable.
func (m *RuleManager) Update(ctx context.Context, id string, rule types.Rule, expectedVersion int64) (types.Rule, error) {
	current, err := m.store.Get(ctx, id)
	if err != nil {
		return types.Rule{}, err
	}
	var immutable []types.FieldError
	if rule.Tenant != current.Tenant {
		immutable = append(immutable, types.FieldError{Field: "tenant", Message: "is immutable; delete and recreate the rule instead"})
	}
	if rule.Namespace != current.Namespace {
		immutable = append(immutable, types.FieldError{Field: "namespace", Message: "is immutable; delete and recreate the rule instead"})
	}
	if len(immutable) > 0 {
		return types.Rule{}, types.NewValidationError(immutable...)
	}
	if current.Version != expectedVersion {
		return types.Rule{}, types.ErrVersionMismatch
	}
	if err := rule.Validate(); err != nil {
		return types.Rule{}, err
	}

	rule.ID = id
	rule.Version = expectedVersion + 1
	rule.CreatedAt = current.CreatedAt
	rule.UpdatedAt = m.now().UTC()
	// The store re-checks the version atomically; the check above only gives
	// a fast, friendly failure.
	if err := m.store.Update(ctx, rule, expectedVersion); err != nil {
		return types.Rule{}, err
	}
	m.cache.Upsert(rule)
	return rule, nil
}

func (m *RuleManager) Delete(ctx context.Context, id string) error {
	if err := m.store.Delete(ctx, id); err != nil {
		return err
	}
	m.cache.Remove(id)
	return nil
}

func (m *RuleManager) Get(ctx context.Context, id string) (types.Rule, error) {
	return m.store.Get(ctx, id)
}

func (m *RuleManager) List(ctx context.Context, f RuleFilter) ([]types.Rule, error) {
	rules, err := m.store.List(ctx)
	if err != nil {
		return nil, err
	}
	out := rules[:0]
	for _, r := range rules {
		if (f.Tenant == "" || r.Tenant == f.Tenant) && (f.Namespace == "" || r.Namespace == f.Namespace) {
			out = append(out, r)
		}
	}
	return out, nil
}
