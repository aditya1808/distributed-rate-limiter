package redisstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
)

// ChangeBus broadcasts rule and assignment changes over Redis pub/sub.
// Delivery is best effort; the rule cache's periodic reload repairs misses.
type ChangeBus struct {
	rdb  redis.UniversalClient
	keys constants.Keyspace
}

var _ persistence.ChangeBus = (*ChangeBus)(nil)

func NewChangeBus(rdb redis.UniversalClient, keys constants.Keyspace) *ChangeBus {
	return &ChangeBus{rdb: rdb, keys: keys}
}

func (b *ChangeBus) Publish(ctx context.Context, ev persistence.ChangeEvent) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if err := b.rdb.Publish(ctx, b.keys.RulesChannel(), data).Err(); err != nil {
		return fmt.Errorf("publish rule change: %w", err)
	}
	return nil
}

func (b *ChangeBus) Subscribe(ctx context.Context) (<-chan persistence.ChangeEvent, error) {
	ps := b.rdb.Subscribe(ctx, b.keys.RulesChannel())

	confirmCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := ps.Receive(confirmCtx); err != nil {
		_ = ps.Close()
		return nil, fmt.Errorf("subscribe to rule changes: %w", err)
	}

	out := make(chan persistence.ChangeEvent, 64)
	go func() {
		defer close(out)
		defer ps.Close()
		msgs := ps.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				var ev persistence.ChangeEvent
				if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
					continue
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}
