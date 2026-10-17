package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/persistence"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

// Each write is one script so the data change and its broadcast cannot be
// separated, and uniqueness / version checks cannot race.

const createScript = `
if redis.call("HSETNX", KEYS[2], ARGV[2], ARGV[1]) == 0 then
  return redis.error_reply("CONFLICT")
end
redis.call("HSET", KEYS[1], ARGV[1], ARGV[3])
redis.call("PUBLISH", ARGV[4], ARGV[5])
return "OK"
`

const updateScript = `
local current = redis.call("HGET", KEYS[1], ARGV[1])
if not current then
  return redis.error_reply("NOT_FOUND")
end
local doc = cjson.decode(current)
if (tonumber(doc.schema_version) or 0) > tonumber(ARGV[6]) then
  return redis.error_reply("UNSUPPORTED_SCHEMA")
end
if tonumber(doc.version) ~= tonumber(ARGV[2]) then
  return redis.error_reply("VERSION_MISMATCH")
end
redis.call("HSET", KEYS[1], ARGV[1], ARGV[3])
redis.call("PUBLISH", ARGV[4], ARGV[5])
return "OK"
`

const deleteScript = `
local current = redis.call("HGET", KEYS[1], ARGV[1])
if not current then
  return redis.error_reply("NOT_FOUND")
end
local rule = cjson.decode(current)
redis.call("HDEL", KEYS[1], ARGV[1])
local pair = rule.tenant .. "|" .. rule.namespace
if redis.call("HGET", KEYS[2], pair) == ARGV[1] then
  redis.call("HDEL", KEYS[2], pair)
end
redis.call("PUBLISH", ARGV[2], ARGV[3])
return "OK"
`

type RuleStore struct {
	rdb    redis.UniversalClient
	keys   constants.Keyspace
	create *redis.Script
	update *redis.Script
	delete *redis.Script
}

var (
	_ persistence.RuleStore        = (*RuleStore)(nil)
	_ persistence.ChangeSubscriber = (*RuleStore)(nil)
)

func NewRuleStore(rdb redis.UniversalClient, keys constants.Keyspace) *RuleStore {
	return &RuleStore{
		rdb:    rdb,
		keys:   keys,
		create: redis.NewScript(createScript),
		update: redis.NewScript(updateScript),
		delete: redis.NewScript(deleteScript),
	}
}

func (s *RuleStore) Create(ctx context.Context, rule types.Rule) error {
	data, event, err := encode(rule, persistence.ChangeUpsert)
	if err != nil {
		return err
	}
	err = s.create.Run(ctx, s.rdb,
		[]string{s.keys.RulesData(), s.keys.RulesIndex()},
		rule.ID, rule.PairKey(), data, s.keys.RulesChannel(), event,
	).Err()
	return mapScriptErr(err)
}

func (s *RuleStore) Update(ctx context.Context, rule types.Rule, expectedVersion int64) error {
	data, event, err := encode(rule, persistence.ChangeUpsert)
	if err != nil {
		return err
	}
	err = s.update.Run(ctx, s.rdb,
		[]string{s.keys.RulesData()},
		rule.ID, expectedVersion, data, s.keys.RulesChannel(), event, RuleSchemaVersion,
	).Err()
	return mapScriptErr(err)
}

func (s *RuleStore) Delete(ctx context.Context, id string) error {
	event, err := json.Marshal(persistence.ChangeEvent{Op: persistence.ChangeDelete, ID: id})
	if err != nil {
		return err
	}
	err = s.delete.Run(ctx, s.rdb,
		[]string{s.keys.RulesData(), s.keys.RulesIndex()},
		id, s.keys.RulesChannel(), event,
	).Err()
	return mapScriptErr(err)
}

func (s *RuleStore) Get(ctx context.Context, id string) (types.Rule, error) {
	data, err := s.rdb.HGet(ctx, s.keys.RulesData(), id).Bytes()
	if errors.Is(err, redis.Nil) {
		return types.Rule{}, types.ErrNotFound
	}
	if err != nil {
		return types.Rule{}, fmt.Errorf("get rule %s: %w", id, err)
	}
	rule, err := decodeRule(data)
	if err != nil {
		return types.Rule{}, fmt.Errorf("decode rule %s: %w", id, err)
	}
	return rule, nil
}

func (s *RuleStore) List(ctx context.Context) ([]types.Rule, error) {
	all, err := s.rdb.HGetAll(ctx, s.keys.RulesData()).Result()
	if err != nil {
		return nil, fmt.Errorf("list rules: %w", err)
	}
	rules := make([]types.Rule, 0, len(all))
	for id, data := range all {
		// Failing the whole list (rather than skipping) keeps the rule cache on
		// its last good snapshot instead of silently dropping a rule.
		rule, err := decodeRule([]byte(data))
		if err != nil {
			return nil, fmt.Errorf("decode rule %s: %w", id, err)
		}
		rules = append(rules, rule)
	}
	sort.Slice(rules, func(i, j int) bool {
		if rules[i].Namespace != rules[j].Namespace {
			return rules[i].Namespace < rules[j].Namespace
		}
		return rules[i].Tenant < rules[j].Tenant
	})
	return rules, nil
}

func (s *RuleStore) Subscribe(ctx context.Context) (<-chan persistence.ChangeEvent, error) {
	ps := s.rdb.Subscribe(ctx, s.keys.RulesChannel())

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

func encode(rule types.Rule, op persistence.ChangeOp) (data, event []byte, err error) {
	data, err = encodeRule(rule)
	if err != nil {
		return nil, nil, fmt.Errorf("encode rule: %w", err)
	}
	event, err = json.Marshal(persistence.ChangeEvent{Op: op, ID: rule.ID, Version: rule.Version})
	if err != nil {
		return nil, nil, fmt.Errorf("encode change event: %w", err)
	}
	return data, event, nil
}

func mapScriptErr(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "CONFLICT"):
		return types.ErrConflict
	case strings.Contains(msg, "NOT_FOUND"):
		return types.ErrNotFound
	case strings.Contains(msg, "VERSION_MISMATCH"):
		return types.ErrVersionMismatch
	case strings.Contains(msg, "UNSUPPORTED_SCHEMA"):
		return types.ErrUnsupportedSchema
	}
	return fmt.Errorf("rule store: %w", err)
}
