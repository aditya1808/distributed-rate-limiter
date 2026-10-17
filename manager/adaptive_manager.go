package manager

import (
	"context"
	"time"

	"github.com/aditya1808/distributed-rate-limiter/persistence"
)

const DefaultAdaptiveTTL = 5 * time.Minute

// MultiplierFor maps a downstream error rate to a limit multiplier.
func MultiplierFor(errorRate float64) float64 {
	switch {
	case errorRate >= 0.5:
		return 0.25
	case errorRate >= 0.2:
		return 0.5
	case errorRate >= 0.05:
		return 0.8
	default:
		return 1
	}
}

type AdaptiveResult struct {
	Multiplier float64
	// ExpiresAt is nil when limits are back to normal.
	ExpiresAt *time.Time
}

// AdaptiveManager tightens a tenant's limits temporarily based on a feedback
// signal. Multipliers expire on their own, so limits relax without any action.
type AdaptiveManager struct {
	store persistence.AdaptiveStore
	now   func() time.Time
}

func NewAdaptiveManager(store persistence.AdaptiveStore) *AdaptiveManager {
	return &AdaptiveManager{store: store, now: time.Now}
}

func (m *AdaptiveManager) Apply(ctx context.Context, tenant, namespace string, errorRate float64, ttl time.Duration) (AdaptiveResult, error) {
	if ttl <= 0 {
		ttl = DefaultAdaptiveTTL
	}
	mult := MultiplierFor(errorRate)
	if mult == 1 {
		if err := m.store.ClearMultiplier(ctx, tenant, namespace); err != nil {
			return AdaptiveResult{}, err
		}
		return AdaptiveResult{Multiplier: 1}, nil
	}
	if err := m.store.SetMultiplier(ctx, tenant, namespace, mult, ttl); err != nil {
		return AdaptiveResult{}, err
	}
	expires := m.now().UTC().Add(ttl)
	return AdaptiveResult{Multiplier: mult, ExpiresAt: &expires}, nil
}
