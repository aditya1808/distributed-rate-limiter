package cache

import (
	"context"
	"time"

	"github.com/aditya1808/distributed-rate-limiter/persistence"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

type Resolver interface {
	Resolve(ctx context.Context, tenant, namespace string) (types.Resolution, error)
}

// SharedLookup is the miss path: the shared Redis resolution cache first,
// then the Postgres source of truth, writing the answer back to Redis.
type SharedLookup struct {
	cache        persistence.ResolutionCache
	store        Resolver
	ttl          time.Duration
	cacheTimeout time.Duration
	storeTimeout time.Duration
}

func NewSharedLookup(c persistence.ResolutionCache, s Resolver, ttl, cacheTimeout, storeTimeout time.Duration) *SharedLookup {
	return &SharedLookup{cache: c, store: s, ttl: ttl, cacheTimeout: cacheTimeout, storeTimeout: storeTimeout}
}

func (l *SharedLookup) Lookup(ctx context.Context, tenant, namespace string) (types.Resolution, string, error) {
	cctx, cancel := context.WithTimeout(ctx, l.cacheTimeout)
	res, ok, err := l.cache.GetResolution(cctx, tenant, namespace)
	cancel()
	if err == nil && ok {
		return res, "redis", nil
	}

	sctx, cancel := context.WithTimeout(ctx, l.storeTimeout)
	res, err = l.store.Resolve(sctx, tenant, namespace)
	cancel()
	if err != nil {
		return types.Resolution{}, "postgres", err
	}

	cctx, cancel = context.WithTimeout(ctx, l.cacheTimeout)
	_ = l.cache.SetResolution(cctx, tenant, namespace, res, l.ttl)
	cancel()
	return res, "postgres", nil
}
