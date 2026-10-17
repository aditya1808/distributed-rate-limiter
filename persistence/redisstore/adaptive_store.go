package redisstore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
)

// AdaptiveStore keeps per-tenant multipliers with a TTL; the limiter scripts
// read them atomically with each decision.
type AdaptiveStore struct {
	rdb  redis.UniversalClient
	keys constants.Keyspace
}

var _ persistence.AdaptiveStore = (*AdaptiveStore)(nil)

func NewAdaptiveStore(rdb redis.UniversalClient, keys constants.Keyspace) *AdaptiveStore {
	return &AdaptiveStore{rdb: rdb, keys: keys}
}

func (s *AdaptiveStore) SetMultiplier(ctx context.Context, tenant, namespace string, mult float64, ttl time.Duration) error {
	key := s.keys.Adaptive(tenant, namespace)
	if err := s.rdb.Set(ctx, key, strconv.FormatFloat(mult, 'f', -1, 64), ttl).Err(); err != nil {
		return fmt.Errorf("set adaptive multiplier: %w", err)
	}
	return nil
}

func (s *AdaptiveStore) ClearMultiplier(ctx context.Context, tenant, namespace string) error {
	if err := s.rdb.Del(ctx, s.keys.Adaptive(tenant, namespace)).Err(); err != nil {
		return fmt.Errorf("clear adaptive multiplier: %w", err)
	}
	return nil
}
