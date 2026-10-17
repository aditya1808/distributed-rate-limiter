// Package persistencetest provides in-memory stand-ins for unit tests.
package persistencetest

import (
	"context"
	"sync"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

// MemoryBindings is an in-memory binding source and change bus for rule
// cache tests.
type MemoryBindings struct {
	mu          sync.Mutex
	bindings    map[string]types.Binding
	subscribers []chan persistence.ChangeEvent
}

var _ persistence.ChangeBus = (*MemoryBindings)(nil)

func NewMemoryBindings() *MemoryBindings {
	return &MemoryBindings{bindings: map[string]types.Binding{}}
}

// Assign binds a rule to a pair and broadcasts the change.
func (s *MemoryBindings) Assign(tenant, namespace string, rule types.Rule) {
	s.AssignSilently(tenant, namespace, rule)
	_ = s.Publish(context.Background(), persistence.ChangeEvent{Kind: persistence.ChangeAssignment})
}

// AssignSilently writes without broadcasting, simulating a lost pub/sub
// message so tests can exercise the periodic reload and miss paths.
func (s *MemoryBindings) AssignSilently(tenant, namespace string, rule types.Rule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bindings[constants.PairKey(tenant, namespace)] = types.Binding{Tenant: tenant, Namespace: namespace, Rule: rule}
}

func (s *MemoryBindings) Unassign(tenant, namespace string) {
	s.mu.Lock()
	delete(s.bindings, constants.PairKey(tenant, namespace))
	s.mu.Unlock()
	_ = s.Publish(context.Background(), persistence.ChangeEvent{Kind: persistence.ChangeAssignment})
}

func (s *MemoryBindings) Bindings(context.Context) ([]types.Binding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]types.Binding, 0, len(s.bindings))
	for _, b := range s.bindings {
		out = append(out, b)
	}
	return out, nil
}

func (s *MemoryBindings) Publish(_ context.Context, ev persistence.ChangeEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.subscribers {
		select {
		case ch <- ev:
		default:
		}
	}
	return nil
}

func (s *MemoryBindings) Subscribe(ctx context.Context) (<-chan persistence.ChangeEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan persistence.ChangeEvent, 64)
	s.subscribers = append(s.subscribers, ch)
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, c := range s.subscribers {
			if c == ch {
				s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
				close(ch)
				return
			}
		}
	}()
	return ch, nil
}
