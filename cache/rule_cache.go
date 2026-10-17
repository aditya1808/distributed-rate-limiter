package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/metrics"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

// RuleCache serves rule lookups from memory on the hot path.
//
// Readers are lock-free: they load an immutable snapshot. Writers (change
// events, periodic reloads, local writes) are serialized by writeMu and only
// ever move a rule forward in version, so a slow reload can never resurrect a
// stale rule.
type RuleCache struct {
	store   persistence.RuleStore
	sub     persistence.ChangeSubscriber
	refresh time.Duration
	metrics *metrics.Metrics
	log     *slog.Logger

	writeMu sync.Mutex
	snap    atomic.Pointer[snapshot]
	loaded  atomic.Bool
	done    chan struct{}
}

type snapshot struct {
	byPair map[string]types.Rule
	byID   map[string]types.Rule
}

func New(store persistence.RuleStore, sub persistence.ChangeSubscriber, refresh time.Duration, m *metrics.Metrics, log *slog.Logger) *RuleCache {
	c := &RuleCache{
		store:   store,
		sub:     sub,
		refresh: refresh,
		metrics: m,
		log:     log,
		done:    make(chan struct{}),
	}
	c.snap.Store(&snapshot{byPair: map[string]types.Rule{}, byID: map[string]types.Rule{}})
	return c
}

// Start subscribes before the initial load so no change can fall between the
// two, then keeps the cache current until ctx is cancelled.
func (c *RuleCache) Start(ctx context.Context) error {
	events, err := c.sub.Subscribe(ctx)
	if err != nil {
		return err
	}
	if err := c.Reload(ctx); err != nil {
		return fmt.Errorf("initial rule load: %w", err)
	}
	c.loaded.Store(true)
	go c.loop(ctx, events)
	return nil
}

// Done is closed when the background loop exits.
func (c *RuleCache) Done() <-chan struct{} { return c.done }

func (c *RuleCache) Loaded() bool { return c.loaded.Load() }

func (c *RuleCache) Resolve(tenant, namespace string) (types.Rule, types.MatchKind, bool) {
	s := c.snap.Load()
	if r, ok := s.byPair[constants.PairKey(tenant, namespace)]; ok {
		return r, types.MatchTenant, true
	}
	if r, ok := s.byPair[constants.PairKey(constants.DefaultTenant, namespace)]; ok {
		return r, types.MatchDefault, true
	}
	return types.Rule{}, "", false
}

func (c *RuleCache) Len() int { return len(c.snap.Load().byID) }

func (c *RuleCache) loop(ctx context.Context, events <-chan persistence.ChangeEvent) {
	defer close(c.done)
	ticker := time.NewTicker(c.refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				// Subscription ended; the periodic reload keeps us converging.
				events = nil
				continue
			}
			c.handle(ctx, ev)
		case <-ticker.C:
			if err := c.Reload(ctx); err != nil && ctx.Err() == nil {
				c.log.Warn("[RULE_CACHE] periodic reload failed", "error", err)
			}
		}
	}
}

func (c *RuleCache) handle(ctx context.Context, ev persistence.ChangeEvent) {
	switch ev.Op {
	case persistence.ChangeDelete:
		c.Remove(ev.ID)
		c.metrics.RuleReloads.WithLabelValues("event").Inc()
	case persistence.ChangeUpsert:
		if cur, ok := c.snap.Load().byID[ev.ID]; ok && cur.Version >= ev.Version {
			return
		}
		getCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		rule, err := c.store.Get(getCtx, ev.ID)
		switch {
		case errors.Is(err, types.ErrNotFound):
			c.Remove(ev.ID)
		case err != nil:
			c.log.Warn("[RULE_CACHE] fetch after change event failed; reload will reconcile", "rule_id", ev.ID, "error", err)
			return
		default:
			c.Upsert(rule)
		}
		c.metrics.RuleReloads.WithLabelValues("event").Inc()
	}
}

// Reload replaces the snapshot with the store's contents, keeping any rule
// this instance already holds at a newer version.
func (c *RuleCache) Reload(ctx context.Context) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	rules, err := c.store.List(ctx)
	if err != nil {
		return err
	}
	current := c.snap.Load()
	next := &snapshot{byPair: make(map[string]types.Rule, len(rules)), byID: make(map[string]types.Rule, len(rules))}
	for _, r := range rules {
		if cur, ok := current.byID[r.ID]; ok && cur.Version > r.Version {
			r = cur
		}
		next.byID[r.ID] = r
		next.byPair[r.PairKey()] = r
	}
	c.snap.Store(next)
	c.metrics.RuleCacheSize.Set(float64(len(next.byID)))
	c.metrics.RuleReloads.WithLabelValues("reload").Inc()
	return nil
}

// Upsert applies a rule if it is newer than what the cache holds.
func (c *RuleCache) Upsert(rule types.Rule) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	current := c.snap.Load()
	if cur, ok := current.byID[rule.ID]; ok && cur.Version >= rule.Version {
		return
	}
	next := current.clone()
	if old, ok := next.byPair[rule.PairKey()]; ok && old.ID != rule.ID {
		delete(next.byID, old.ID)
	}
	next.byID[rule.ID] = rule
	next.byPair[rule.PairKey()] = rule
	c.snap.Store(next)
	c.metrics.RuleCacheSize.Set(float64(len(next.byID)))
}

func (c *RuleCache) Remove(id string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	current := c.snap.Load()
	rule, ok := current.byID[id]
	if !ok {
		return
	}
	next := current.clone()
	delete(next.byID, id)
	if next.byPair[rule.PairKey()].ID == id {
		delete(next.byPair, rule.PairKey())
	}
	c.snap.Store(next)
	c.metrics.RuleCacheSize.Set(float64(len(next.byID)))
}

func (s *snapshot) clone() *snapshot {
	return &snapshot{byPair: maps.Clone(s.byPair), byID: maps.Clone(s.byID)}
}
