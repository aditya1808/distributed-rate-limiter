package cache

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/metrics"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

type BindingSource interface {
	Bindings(ctx context.Context) ([]types.Binding, error)
}

type Subscriber interface {
	Subscribe(ctx context.Context) (<-chan persistence.ChangeEvent, error)
}

// Lookup answers for a pair the snapshot does not cover, reporting where the
// answer came from (for metrics).
type Lookup interface {
	Lookup(ctx context.Context, tenant, namespace string) (types.Resolution, string, error)
}

type Options struct {
	// Refresh is the full reload interval that repairs missed change events.
	Refresh time.Duration
	// MissTTL is how long a fallback answer, including "no rule", is reused.
	MissTTL time.Duration
}

// maxMisses bounds memory when traffic spreads over many unmatched pairs.
const maxMisses = 100_000

// RuleCache serves rule lookups from memory on the hot path.
//
// Every instance holds a snapshot of all assignments joined with their rules,
// replaced wholesale on change events and periodic reloads. Readers are
// lock-free. A pair the snapshot has no rule for falls back to Lookup
// (Redis, then Postgres); those answers are kept for MissTTL and dropped on
// every reload, so the snapshot stays authoritative.
type RuleCache struct {
	source   BindingSource
	sub      Subscriber
	fallback Lookup
	opts     Options
	metrics  *metrics.Metrics
	log      *slog.Logger

	reloadMu sync.Mutex
	snap     atomic.Pointer[snapshot]
	gen      atomic.Uint64
	loaded   atomic.Bool
	done     chan struct{}

	missMu sync.Mutex
	misses map[string]missEntry
	flight singleflight.Group

	lastWarn atomic.Int64
}

type snapshot struct {
	byPair map[string]types.Rule
}

type missEntry struct {
	res     types.Resolution
	expires time.Time
}

func New(source BindingSource, sub Subscriber, fallback Lookup, opts Options, m *metrics.Metrics, log *slog.Logger) *RuleCache {
	c := &RuleCache{
		source:   source,
		sub:      sub,
		fallback: fallback,
		opts:     opts,
		metrics:  m,
		log:      log,
		done:     make(chan struct{}),
		misses:   map[string]missEntry{},
	}
	c.snap.Store(&snapshot{byPair: map[string]types.Rule{}})
	return c
}

// Start subscribes before the initial load so no change can fall between the
// two, then keeps the cache current until ctx is cancelled.
func (c *RuleCache) Start(ctx context.Context) error {
	events, err := c.sub.Subscribe(ctx)
	if err != nil {
		return err
	}
	if err := c.reload(ctx, "initial"); err != nil {
		return fmt.Errorf("initial rule load: %w", err)
	}
	c.loaded.Store(true)
	go c.loop(ctx, events)
	return nil
}

// Done is closed when the background loop exits.
func (c *RuleCache) Done() <-chan struct{} { return c.done }

func (c *RuleCache) Loaded() bool { return c.loaded.Load() }

// Len is the number of assignments in the snapshot.
func (c *RuleCache) Len() int { return len(c.snap.Load().byPair) }

func (c *RuleCache) Resolve(ctx context.Context, tenant, namespace string) (types.Rule, types.MatchKind, bool) {
	s := c.snap.Load()
	if r, ok := s.byPair[constants.PairKey(tenant, namespace)]; ok {
		return r, types.MatchTenant, true
	}
	if r, ok := s.byPair[constants.PairKey(constants.DefaultTenant, namespace)]; ok {
		return r, types.MatchDefault, true
	}
	res := c.miss(ctx, tenant, namespace)
	return res.Rule, res.Match, res.Found
}

func (c *RuleCache) miss(ctx context.Context, tenant, namespace string) types.Resolution {
	if c.fallback == nil {
		return types.Resolution{}
	}
	key := constants.PairKey(tenant, namespace)
	if res, ok := c.getMiss(key); ok {
		c.metrics.RuleMisses.WithLabelValues("local").Inc()
		return res
	}
	gen := c.gen.Load()
	v, _, _ := c.flight.Do(key, func() (any, error) {
		// Followers share the leader's answer, so the leader's cancellation
		// must not fail them; Lookup applies its own per-step timeouts.
		res, source, err := c.fallback.Lookup(context.WithoutCancel(ctx), tenant, namespace)
		if err != nil {
			// Cache the failure too, so an outage costs one lookup per pair
			// per MissTTL instead of one per request.
			c.warnSampled("[RULE_CACHE] fallback lookup failed; treating as no rule", "error", err)
			source, res = "error", types.Resolution{}
		}
		c.metrics.RuleMisses.WithLabelValues(source).Inc()
		c.putMiss(key, res, gen)
		return res, nil
	})
	return v.(types.Resolution)
}

func (c *RuleCache) getMiss(key string) (types.Resolution, bool) {
	c.missMu.Lock()
	defer c.missMu.Unlock()
	e, ok := c.misses[key]
	if !ok || time.Now().After(e.expires) {
		return types.Resolution{}, false
	}
	return e.res, true
}

// putMiss skips answers that raced a reload: they may predate it.
func (c *RuleCache) putMiss(key string, res types.Resolution, gen uint64) {
	c.missMu.Lock()
	defer c.missMu.Unlock()
	if c.gen.Load() != gen {
		return
	}
	if len(c.misses) >= maxMisses {
		clear(c.misses)
	}
	c.misses[key] = missEntry{res: res, expires: time.Now().Add(c.opts.MissTTL)}
}

func (c *RuleCache) loop(ctx context.Context, events <-chan persistence.ChangeEvent) {
	defer close(c.done)
	ticker := time.NewTicker(c.opts.Refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-events:
			if !ok {
				// Subscription ended; the periodic reload keeps us converging.
				events = nil
				continue
			}
			// One reload covers every event queued so far.
			for drained := false; !drained; {
				select {
				case <-events:
				default:
					drained = true
				}
			}
			if err := c.reload(ctx, "event"); err != nil && ctx.Err() == nil {
				c.log.Warn("[RULE_CACHE] reload after change event failed; periodic reload will retry", "error", err)
			}
		case <-ticker.C:
			if err := c.reload(ctx, "periodic"); err != nil && ctx.Err() == nil {
				c.log.Warn("[RULE_CACHE] periodic reload failed", "error", err)
			}
		}
	}
}

// Reload replaces the snapshot with the store's current contents. Writers
// call it after committing so this instance enforces its own writes at once.
func (c *RuleCache) Reload(ctx context.Context) error { return c.reload(ctx, "write") }

func (c *RuleCache) reload(ctx context.Context, trigger string) error {
	// Serialized, with the query inside the lock: the last reload to finish
	// is always the one that read last, so the snapshot never moves backwards.
	c.reloadMu.Lock()
	defer c.reloadMu.Unlock()

	bindings, err := c.source.Bindings(ctx)
	if err != nil {
		return err
	}
	next := &snapshot{byPair: make(map[string]types.Rule, len(bindings))}
	for _, b := range bindings {
		next.byPair[b.PairKey()] = b.Rule
	}
	c.snap.Store(next)

	c.missMu.Lock()
	c.gen.Add(1)
	clear(c.misses)
	c.missMu.Unlock()

	c.metrics.RuleCacheSize.Set(float64(len(next.byPair)))
	c.metrics.RuleReloads.WithLabelValues(trigger).Inc()
	return nil
}

// warnSampled logs at most once per second so an outage cannot flood logs.
func (c *RuleCache) warnSampled(msg string, args ...any) {
	now := time.Now().Unix()
	last := c.lastWarn.Load()
	if now > last && c.lastWarn.CompareAndSwap(last, now) {
		c.log.Warn(msg, args...)
	}
}
