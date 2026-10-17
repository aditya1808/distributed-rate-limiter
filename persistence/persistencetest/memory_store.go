// Package persistencetest provides in-memory stores for unit tests.
package persistencetest

import (
	"context"
	"sort"
	"sync"

	"github.com/aditya1808/distributed-rate-limiter/persistence"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

type MemoryRuleStore struct {
	mu          sync.Mutex
	rules       map[string]types.Rule
	index       map[string]string
	subscribers []chan persistence.ChangeEvent
}

var (
	_ persistence.RuleStore        = (*MemoryRuleStore)(nil)
	_ persistence.ChangeSubscriber = (*MemoryRuleStore)(nil)
)

func NewMemoryRuleStore() *MemoryRuleStore {
	return &MemoryRuleStore{rules: map[string]types.Rule{}, index: map[string]string{}}
}

func (s *MemoryRuleStore) Create(_ context.Context, rule types.Rule) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.index[rule.PairKey()]; exists {
		return types.ErrConflict
	}
	s.index[rule.PairKey()] = rule.ID
	s.rules[rule.ID] = rule
	s.publish(persistence.ChangeEvent{Op: persistence.ChangeUpsert, ID: rule.ID, Version: rule.Version})
	return nil
}

func (s *MemoryRuleStore) Update(_ context.Context, rule types.Rule, expectedVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.rules[rule.ID]
	if !ok {
		return types.ErrNotFound
	}
	if current.Version != expectedVersion {
		return types.ErrVersionMismatch
	}
	s.rules[rule.ID] = rule
	s.publish(persistence.ChangeEvent{Op: persistence.ChangeUpsert, ID: rule.ID, Version: rule.Version})
	return nil
}

func (s *MemoryRuleStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.rules[id]
	if !ok {
		return types.ErrNotFound
	}
	delete(s.rules, id)
	if s.index[current.PairKey()] == id {
		delete(s.index, current.PairKey())
	}
	s.publish(persistence.ChangeEvent{Op: persistence.ChangeDelete, ID: id})
	return nil
}

func (s *MemoryRuleStore) Get(_ context.Context, id string) (types.Rule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rule, ok := s.rules[id]
	if !ok {
		return types.Rule{}, types.ErrNotFound
	}
	return rule, nil
}

func (s *MemoryRuleStore) List(_ context.Context) ([]types.Rule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]types.Rule, 0, len(s.rules))
	for _, r := range s.rules {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// PutSilently writes a rule without publishing, simulating a lost pub/sub
// message so tests can exercise the periodic reload path.
func (s *MemoryRuleStore) PutSilently(rule types.Rule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules[rule.ID] = rule
	s.index[rule.PairKey()] = rule.ID
}

func (s *MemoryRuleStore) Subscribe(ctx context.Context) (<-chan persistence.ChangeEvent, error) {
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

func (s *MemoryRuleStore) publish(ev persistence.ChangeEvent) {
	for _, ch := range s.subscribers {
		select {
		case ch <- ev:
		default:
		}
	}
}
