package manager

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

type LocalRuleCache interface {
	Reload(ctx context.Context) error
}

// RuleManager owns the lifecycle of rules and assignments. Writes commit to
// Postgres, reload this instance's cache (so it enforces its own writes
// immediately) and broadcast a change event so every other instance reloads.
// Reads go to Postgres, not the cache, so admins always read their writes.
type RuleManager struct {
	store       persistence.RuleStore
	cache       LocalRuleCache
	bus         persistence.ChangeBus
	resolutions persistence.ResolutionCache
	log         *slog.Logger
	now         func() time.Time
}

func NewRuleManager(store persistence.RuleStore, cache LocalRuleCache, bus persistence.ChangeBus,
	resolutions persistence.ResolutionCache, log *slog.Logger) *RuleManager {
	return &RuleManager{store: store, cache: cache, bus: bus, resolutions: resolutions, log: log, now: time.Now}
}

func (m *RuleManager) CreateRule(ctx context.Context, rule types.Rule) (types.Rule, error) {
	if err := rule.Validate(); err != nil {
		return types.Rule{}, err
	}
	now := m.now().UTC().Truncate(time.Microsecond)
	rule.ID = "r_" + strings.ToLower(ulid.Make().String())
	rule.Version = 1
	rule.CreatedAt = now
	rule.UpdatedAt = now
	if err := m.store.CreateRule(ctx, rule); err != nil {
		return types.Rule{}, err
	}
	// An unassigned rule affects no traffic, so there is nothing to broadcast.
	return rule, nil
}

// UpdateRule replaces a rule if expectedVersion matches. Every pair assigned
// to the rule starts fresh quota state under the new version.
func (m *RuleManager) UpdateRule(ctx context.Context, id string, rule types.Rule, expectedVersion int64) (types.Rule, error) {
	if err := rule.Validate(); err != nil {
		return types.Rule{}, err
	}
	current, err := m.store.GetRule(ctx, id)
	if err != nil {
		return types.Rule{}, err
	}
	if current.Version != expectedVersion {
		return types.Rule{}, types.ErrVersionMismatch
	}
	rule.ID = id
	rule.Version = expectedVersion + 1
	rule.CreatedAt = current.CreatedAt
	rule.UpdatedAt = m.now().UTC().Truncate(time.Microsecond)
	// The store re-checks the version atomically; the check above only gives
	// a fast, friendly failure.
	if err := m.store.UpdateRule(ctx, rule, expectedVersion); err != nil {
		return types.Rule{}, err
	}
	m.changed(ctx, persistence.ChangeEvent{Kind: persistence.ChangeRule, ID: id})
	return rule, nil
}

func (m *RuleManager) DeleteRule(ctx context.Context, id string) error {
	return m.store.DeleteRule(ctx, id)
}

func (m *RuleManager) GetRule(ctx context.Context, id string) (types.Rule, error) {
	return m.store.GetRule(ctx, id)
}

func (m *RuleManager) ListRules(ctx context.Context) ([]types.Rule, error) {
	return m.store.ListRules(ctx)
}

// Assign binds a rule to a pair, replacing any existing assignment.
func (m *RuleManager) Assign(ctx context.Context, a types.Assignment) (types.Assignment, bool, error) {
	if err := a.Validate(); err != nil {
		return types.Assignment{}, false, err
	}
	a.UpdatedAt = m.now().UTC().Truncate(time.Microsecond)
	saved, created, err := m.store.PutAssignment(ctx, a)
	if err != nil {
		return types.Assignment{}, false, err
	}
	m.forget(ctx, a.Tenant, a.Namespace)
	m.changed(ctx, persistence.ChangeEvent{Kind: persistence.ChangeAssignment, ID: a.PairKey()})
	return saved, created, nil
}

func (m *RuleManager) Unassign(ctx context.Context, tenant, namespace string) error {
	if err := m.store.DeleteAssignment(ctx, tenant, namespace); err != nil {
		return err
	}
	m.forget(ctx, tenant, namespace)
	m.changed(ctx, persistence.ChangeEvent{Kind: persistence.ChangeAssignment, ID: constants.PairKey(tenant, namespace)})
	return nil
}

func (m *RuleManager) GetAssignment(ctx context.Context, tenant, namespace string) (types.Assignment, error) {
	return m.store.GetAssignment(ctx, tenant, namespace)
}

func (m *RuleManager) ListAssignments(ctx context.Context, f persistence.AssignmentFilter) ([]types.Assignment, error) {
	return m.store.ListAssignments(ctx, f)
}

// changed runs after a committed write. Failures here are logged, not
// returned: the write succeeded, and the periodic reload converges every
// instance within RULE_REFRESH_INTERVAL.
func (m *RuleManager) changed(ctx context.Context, ev persistence.ChangeEvent) {
	ctx = context.WithoutCancel(ctx)
	if err := m.cache.Reload(ctx); err != nil {
		m.log.Warn("[RULE_MANAGER] local cache reload after write failed", "error", err)
	}
	if err := m.bus.Publish(ctx, ev); err != nil {
		m.log.Warn("[RULE_MANAGER] change broadcast failed; instances converge on periodic reload", "error", err)
	}
}

// forget drops the shared cached resolution for the pair itself. Tenants that
// resolved through a "*" assignment expire with RULE_MISS_TTL.
func (m *RuleManager) forget(ctx context.Context, tenant, namespace string) {
	if err := m.resolutions.DeleteResolution(context.WithoutCancel(ctx), tenant, namespace); err != nil {
		m.log.Warn("[RULE_MANAGER] dropping cached resolution failed", "error", err)
	}
}
