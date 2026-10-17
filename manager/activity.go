package manager

import (
	"context"
	"log/slog"
	"time"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
)

const (
	activityFlushInterval = time.Second
	activityRetention     = time.Hour
	activityBuffer        = 4096
)

// Activity records which (tenant, namespace) pairs are active for the
// dashboard. Touch never blocks the hot path: entries are buffered, deduped
// and flushed in one batch per second; when the buffer is full they are
// dropped, which is acceptable for a best-effort view.
type Activity struct {
	store persistence.ActivityStore
	ch    chan string
	log   *slog.Logger
	done  chan struct{}
}

func NewActivity(store persistence.ActivityStore, log *slog.Logger) *Activity {
	return &Activity{store: store, ch: make(chan string, activityBuffer), log: log, done: make(chan struct{})}
}

func (a *Activity) Touch(tenant, namespace string) {
	select {
	case a.ch <- constants.PairKey(tenant, namespace):
	default:
	}
}

func (a *Activity) Done() <-chan struct{} { return a.done }

func (a *Activity) Run(ctx context.Context) {
	defer close(a.done)
	ticker := time.NewTicker(activityFlushInterval)
	defer ticker.Stop()
	pending := map[string]time.Time{}
	ticks := 0
	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			a.flush(flushCtx, pending)
			cancel()
			return
		case pair := <-a.ch:
			pending[pair] = time.Now()
		case <-ticker.C:
			flushCtx, cancel := context.WithTimeout(ctx, time.Second)
			a.flush(flushCtx, pending)
			ticks++
			if ticks%60 == 0 {
				if err := a.store.Trim(flushCtx, time.Now().Add(-activityRetention)); err != nil {
					a.log.Debug("[ACTIVITY] trim failed", "error", err)
				}
			}
			cancel()
			pending = map[string]time.Time{}
		}
	}
}

func (a *Activity) flush(ctx context.Context, pending map[string]time.Time) {
	if len(pending) == 0 {
		return
	}
	if err := a.store.Record(ctx, pending); err != nil {
		a.log.Debug("[ACTIVITY] flush failed", "error", err)
	}
}

func (a *Activity) Recent(ctx context.Context, limit int) ([]persistence.ActivityEntry, error) {
	return a.store.Recent(ctx, limit)
}
