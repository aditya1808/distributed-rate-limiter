package redisstore

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
)

// ActivityStore tracks recently active (tenant, namespace) pairs in a sorted
// set so the dashboard never has to SCAN the keyspace.
type ActivityStore struct {
	rdb  redis.UniversalClient
	keys constants.Keyspace
}

var _ persistence.ActivityStore = (*ActivityStore)(nil)

func NewActivityStore(rdb redis.UniversalClient, keys constants.Keyspace) *ActivityStore {
	return &ActivityStore{rdb: rdb, keys: keys}
}

func (s *ActivityStore) Record(ctx context.Context, seen map[string]time.Time) error {
	if len(seen) == 0 {
		return nil
	}
	members := make([]redis.Z, 0, len(seen))
	for pair, at := range seen {
		members = append(members, redis.Z{Score: float64(at.UnixMilli()), Member: pair})
	}
	if err := s.rdb.ZAdd(ctx, s.keys.Activity(), members...).Err(); err != nil {
		return fmt.Errorf("record activity: %w", err)
	}
	return nil
}

func (s *ActivityStore) Recent(ctx context.Context, limit int) ([]persistence.ActivityEntry, error) {
	zs, err := s.rdb.ZRevRangeWithScores(ctx, s.keys.Activity(), 0, int64(limit-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("recent activity: %w", err)
	}
	out := make([]persistence.ActivityEntry, 0, len(zs))
	for _, z := range zs {
		pair, _ := z.Member.(string)
		tenant, namespace, ok := strings.Cut(pair, "|")
		if !ok {
			continue
		}
		out = append(out, persistence.ActivityEntry{
			Tenant:    tenant,
			Namespace: namespace,
			LastSeen:  time.UnixMilli(int64(z.Score)).UTC(),
		})
	}
	return out, nil
}

func (s *ActivityStore) Trim(ctx context.Context, olderThan time.Time) error {
	maxScore := "(" + strconv.FormatInt(olderThan.UnixMilli(), 10)
	if err := s.rdb.ZRemRangeByScore(ctx, s.keys.Activity(), "-inf", maxScore).Err(); err != nil {
		return fmt.Errorf("trim activity: %w", err)
	}
	return nil
}
