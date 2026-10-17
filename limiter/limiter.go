package limiter

import (
	"context"
	"time"

	"github.com/aditya1808/distributed-rate-limiter/types"
)

type Request struct {
	Rule        types.Rule
	StateKey    string
	SeqKey      string
	AdaptiveKey string
	Cost        int64
	// Peek computes the current state without consuming or writing anything.
	Peek bool
}

type Result struct {
	Allowed    bool
	Limit      int64
	Used       int64
	Remaining  int64
	RetryAfter time.Duration
	ResetAfter time.Duration
	// Now is the state store's clock, so every instance reports identical
	// reset times regardless of local clock skew.
	Now        time.Time
	Multiplier float64
}

type Limiter interface {
	Evaluate(ctx context.Context, req Request) (Result, error)
}

func refillPerMs(r types.Rule) float64 {
	return float64(r.Limit) / float64(r.Period.Milliseconds())
}
