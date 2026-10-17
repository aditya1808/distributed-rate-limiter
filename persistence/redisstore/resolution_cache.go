package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

// ResolutionCache stores Resolve results, including "no rule", with a short
// TTL so a fleet of instances asks Postgres about a pair at most once per TTL.
type ResolutionCache struct {
	rdb  redis.UniversalClient
	keys constants.Keyspace
}

var _ persistence.ResolutionCache = (*ResolutionCache)(nil)

func NewResolutionCache(rdb redis.UniversalClient, keys constants.Keyspace) *ResolutionCache {
	return &ResolutionCache{rdb: rdb, keys: keys}
}

func (c *ResolutionCache) GetResolution(ctx context.Context, tenant, namespace string) (types.Resolution, bool, error) {
	data, err := c.rdb.Get(ctx, c.keys.Resolution(tenant, namespace)).Bytes()
	if errors.Is(err, redis.Nil) {
		return types.Resolution{}, false, nil
	}
	if err != nil {
		return types.Resolution{}, false, fmt.Errorf("get resolution: %w", err)
	}
	var res types.Resolution
	if err := json.Unmarshal(data, &res); err != nil {
		return types.Resolution{}, false, fmt.Errorf("decode resolution: %w", err)
	}
	return res, true, nil
}

func (c *ResolutionCache) SetResolution(ctx context.Context, tenant, namespace string, res types.Resolution, ttl time.Duration) error {
	data, err := json.Marshal(res)
	if err != nil {
		return err
	}
	if err := c.rdb.Set(ctx, c.keys.Resolution(tenant, namespace), data, ttl).Err(); err != nil {
		return fmt.Errorf("set resolution: %w", err)
	}
	return nil
}

func (c *ResolutionCache) DeleteResolution(ctx context.Context, tenant, namespace string) error {
	if err := c.rdb.Del(ctx, c.keys.Resolution(tenant, namespace)).Err(); err != nil {
		return fmt.Errorf("delete resolution: %w", err)
	}
	return nil
}
