package manager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiplierFor(t *testing.T) {
	cases := map[float64]float64{0: 1, 0.049: 1, 0.05: 0.8, 0.19: 0.8, 0.2: 0.5, 0.49: 0.5, 0.5: 0.25, 1: 0.25}
	for rate, want := range cases {
		assert.Equal(t, want, MultiplierFor(rate), "error rate %v", rate)
	}
}

type fakeAdaptiveStore struct {
	set     map[string]float64
	ttl     time.Duration
	cleared int
}

func (f *fakeAdaptiveStore) SetMultiplier(_ context.Context, tenant, ns string, mult float64, ttl time.Duration) error {
	f.set[tenant+"|"+ns] = mult
	f.ttl = ttl
	return nil
}

func (f *fakeAdaptiveStore) ClearMultiplier(_ context.Context, tenant, ns string) error {
	delete(f.set, tenant+"|"+ns)
	f.cleared++
	return nil
}

func TestAdaptiveApply(t *testing.T) {
	store := &fakeAdaptiveStore{set: map[string]float64{}}
	m := NewAdaptiveManager(store)
	fixed := time.UnixMilli(1_700_000_000_000).UTC()
	m.now = func() time.Time { return fixed }

	res, err := m.Apply(context.Background(), "acme", "orders", 0.3, 0)
	require.NoError(t, err)
	assert.Equal(t, 0.5, res.Multiplier)
	assert.Equal(t, DefaultAdaptiveTTL, store.ttl)
	require.NotNil(t, res.ExpiresAt)
	assert.Equal(t, fixed.Add(DefaultAdaptiveTTL), *res.ExpiresAt)

	res, err = m.Apply(context.Background(), "acme", "orders", 0.01, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 1.0, res.Multiplier)
	assert.Nil(t, res.ExpiresAt)
	assert.Equal(t, 1, store.cleared)
	assert.Empty(t, store.set)
}
