package limiter

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/aditya1808/distributed-rate-limiter/types"
)

// Clock returns the current time. Tests inject a fake one.
type Clock func() time.Time

// MemoryLimiter is a single-process reference implementation of the same
// algorithms as the Lua scripts. It backs unit tests (with a fake clock) and
// the contract tests that check the Lua scripts against it.
type MemoryLimiter struct {
	mu          sync.Mutex
	now         Clock
	buckets     map[string]bucket
	windows     map[string][]int64
	multipliers map[string]float64
}

type bucket struct {
	tokens float64
	ts     int64
}

func NewMemoryLimiter(now Clock) *MemoryLimiter {
	if now == nil {
		now = time.Now
	}
	return &MemoryLimiter{
		now:         now,
		buckets:     map[string]bucket{},
		windows:     map[string][]int64{},
		multipliers: map[string]float64{},
	}
}

func (m *MemoryLimiter) SetMultiplier(adaptiveKey string, mult float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mult == 1 {
		delete(m.multipliers, adaptiveKey)
		return
	}
	m.multipliers[adaptiveKey] = mult
}

func (m *MemoryLimiter) Evaluate(ctx context.Context, req Request) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	mult, ok := m.multipliers[req.AdaptiveKey]
	if !ok {
		mult = 1
	}
	now := m.now().UnixMilli()

	switch req.Rule.Algorithm {
	case types.AlgorithmTokenBucket:
		return m.tokenBucket(req, now, mult), nil
	case types.AlgorithmSlidingWindow:
		return m.slidingWindow(req, now, mult), nil
	}
	return Result{}, fmt.Errorf("unknown algorithm %q", req.Rule.Algorithm)
}

func (m *MemoryLimiter) tokenBucket(req Request, now int64, mult float64) Result {
	capacity := math.Max(1, math.Floor(float64(req.Rule.Capacity())*mult))
	rate := refillPerMs(req.Rule) * mult
	cost := float64(req.Cost)

	b, ok := m.buckets[req.StateKey]
	if !ok {
		b = bucket{tokens: capacity, ts: now}
	}
	tokens := b.tokens
	if now > b.ts {
		tokens += float64(now-b.ts) * rate
	}
	tokens = round6(math.Min(tokens, capacity))

	allowed := false
	if tokens+1e-9 >= cost {
		allowed = true
		if !req.Peek {
			tokens -= cost
		}
	}
	tokens = math.Max(tokens, 0)

	var retryAfter float64
	if !allowed {
		retryAfter = math.Ceil((cost - tokens) / rate)
	}
	resetAfter := math.Ceil((capacity - tokens) / rate)

	if !req.Peek {
		m.buckets[req.StateKey] = bucket{tokens: round6(tokens), ts: now}
	}

	remaining := int64(math.Floor(tokens + 1e-9))
	return Result{
		Allowed:    allowed,
		Limit:      int64(capacity),
		Used:       int64(capacity) - remaining,
		Remaining:  remaining,
		RetryAfter: time.Duration(retryAfter) * time.Millisecond,
		ResetAfter: time.Duration(resetAfter) * time.Millisecond,
		Now:        time.UnixMilli(now).UTC(),
		Multiplier: mult,
	}
}

func (m *MemoryLimiter) slidingWindow(req Request, now int64, mult float64) Result {
	limit := int64(math.Max(1, math.Floor(float64(req.Rule.Limit)*mult)))
	burst := req.Rule.Burst
	if burst > 0 {
		burst = int64(math.Max(1, math.Floor(float64(burst)*mult)))
	}
	window := req.Rule.Period.Milliseconds()
	burstWindow := req.Rule.BurstWindow.Milliseconds()
	windowStart := now - window

	entries := m.windows[req.StateKey]
	if !req.Peek {
		entries = entries[sort.Search(len(entries), func(i int) bool { return entries[i] > windowStart }):]
	}
	expired := int64(sort.Search(len(entries), func(i int) bool { return entries[i] > windowStart }))
	used := int64(len(entries)) - expired
	var burstUsed int64
	if burst > 0 {
		burstStart := now - burstWindow
		burstUsed = int64(len(entries) - sort.Search(len(entries), func(i int) bool { return entries[i] > burstStart }))
	}

	cost := req.Cost
	allowed := used+cost <= limit && (burst == 0 || burstUsed+cost <= burst)

	scoreAt := func(rank int64) (int64, bool) {
		if rank < 0 || rank >= int64(len(entries)) {
			return 0, false
		}
		return entries[rank], true
	}

	var retryAfter int64
	if !allowed {
		if used+cost > limit {
			if s, ok := scoreAt(expired + used + cost - limit - 1); ok {
				retryAfter = max(retryAfter, s+window-now)
			} else {
				retryAfter = max(retryAfter, window)
			}
		}
		if burst > 0 && burstUsed+cost > burst {
			firstInBurst := expired + used - burstUsed
			if s, ok := scoreAt(firstInBurst + burstUsed + cost - burst - 1); ok {
				retryAfter = max(retryAfter, s+burstWindow-now)
			} else {
				retryAfter = max(retryAfter, burstWindow)
			}
		}
	}

	if allowed && !req.Peek {
		for range cost {
			entries = append(entries, now)
		}
		used += cost
		burstUsed += cost
	}
	if !req.Peek {
		m.windows[req.StateKey] = entries
	}

	remaining := limit - used
	if burst > 0 && burst-burstUsed < remaining {
		remaining = burst - burstUsed
	}
	remaining = max(remaining, 0)

	var resetAfter int64
	if len(entries) > 0 {
		resetAfter = max(0, entries[len(entries)-1]+window-now)
	}

	return Result{
		Allowed:    allowed,
		Limit:      limit,
		Used:       used,
		Remaining:  remaining,
		RetryAfter: time.Duration(retryAfter) * time.Millisecond,
		ResetAfter: time.Duration(resetAfter) * time.Millisecond,
		Now:        time.UnixMilli(now).UTC(),
		Multiplier: mult,
	}
}

func round6(v float64) float64 { return math.Round(v*1e6) / 1e6 }
