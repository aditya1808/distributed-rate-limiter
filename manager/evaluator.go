package manager

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/limiter"
	"github.com/aditya1808/distributed-rate-limiter/metrics"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

type RuleResolver interface {
	Resolve(ctx context.Context, tenant, namespace string) (types.Rule, types.MatchKind, bool)
}

type ActivityRecorder interface {
	Touch(tenant, namespace string)
}

type EvaluatorConfig struct {
	// DefaultAllow is the decision when no rule matches.
	DefaultAllow bool
	// StoreTimeout bounds every state store call on the hot path.
	StoreTimeout time.Duration
}

type Evaluator struct {
	rules    RuleResolver
	limiter  limiter.Limiter
	activity ActivityRecorder
	metrics  *metrics.Metrics
	keys     constants.Keyspace
	log      *slog.Logger
	cfg      EvaluatorConfig

	lastWarn atomic.Int64
}

func NewEvaluator(rules RuleResolver, lim limiter.Limiter, activity ActivityRecorder, m *metrics.Metrics,
	keys constants.Keyspace, log *slog.Logger, cfg EvaluatorConfig) *Evaluator {
	return &Evaluator{rules: rules, limiter: lim, activity: activity, metrics: m, keys: keys, log: log, cfg: cfg}
}

// Evaluate decides and consumes quota. A state store failure is not an error:
// it yields a degraded decision following the rule's fail_open setting, so
// callers always get an answer within the store timeout.
func (e *Evaluator) Evaluate(ctx context.Context, tenant, namespace string, cost int64) (types.Decision, error) {
	d := types.Decision{Tenant: tenant, Namespace: namespace, Multiplier: 1}
	// Every decision is activity, including unmatched traffic: the dashboard
	// is where operators notice tenants that are missing a rule.
	e.activity.Touch(tenant, namespace)

	rule, _, ok := e.rules.Resolve(ctx, tenant, namespace)
	if !ok {
		d.Allowed = e.cfg.DefaultAllow
		d.Reason = types.ReasonNoRule
		e.metrics.RecordDecision("_unmatched", "none", d.Allowed, d.Reason)
		return d, nil
	}
	d.RuleID = rule.ID
	d.Algorithm = rule.Algorithm
	d.Limit = rule.Capacity()

	if cost > rule.Capacity() {
		d.Reason = types.ReasonCostExceedsCapacity
		e.metrics.RecordDecision(namespace, string(rule.Algorithm), false, d.Reason)
		return d, nil
	}

	res, err := e.call(ctx, "evaluate", e.request(rule, tenant, namespace, cost, false))
	if err != nil {
		if ctx.Err() != nil {
			return d, ctx.Err()
		}
		e.warnSampled("[EVALUATOR] state store unavailable, applying fail mode",
			"error", err, "rule_id", rule.ID, "fail_open", rule.FailOpen)
		d.Allowed = rule.FailOpen
		d.Degraded = true
		d.Reason = types.ReasonStoreUnavailable
		e.metrics.RecordDecision(namespace, string(rule.Algorithm), d.Allowed, d.Reason)
		return d, nil
	}

	d.Allowed = res.Allowed
	d.Limit = res.Limit
	d.Remaining = res.Remaining
	d.ResetAt = res.Now.Add(res.ResetAfter)
	d.RetryAfter = res.RetryAfter
	d.Multiplier = res.Multiplier
	if !res.Allowed {
		d.Reason = types.ReasonQuotaExceeded
	}
	e.metrics.RecordDecision(namespace, string(rule.Algorithm), d.Allowed, d.Reason)
	return d, nil
}

// Peek reports current quota without consuming it. Unlike Evaluate, store
// failures are returned: an observation has no safe fallback value.
func (e *Evaluator) Peek(ctx context.Context, tenant, namespace string) (types.Quota, error) {
	rule, match, ok := e.rules.Resolve(ctx, tenant, namespace)
	if !ok {
		return types.Quota{}, types.ErrNoRule
	}
	res, err := e.call(ctx, "peek", e.request(rule, tenant, namespace, 1, true))
	if err != nil {
		if ctx.Err() != nil {
			return types.Quota{}, ctx.Err()
		}
		return types.Quota{}, fmt.Errorf("%w: %w", types.ErrStoreUnavailable, err)
	}
	return types.Quota{
		Tenant:     tenant,
		Namespace:  namespace,
		Rule:       rule,
		Match:      match,
		Limit:      res.Limit,
		Used:       res.Used,
		Remaining:  res.Remaining,
		ResetAt:    res.Now.Add(res.ResetAfter),
		Multiplier: res.Multiplier,
	}, nil
}

func (e *Evaluator) request(rule types.Rule, tenant, namespace string, cost int64, peek bool) limiter.Request {
	// The real tenant is always part of the key: a "*" default rule gives each
	// tenant its own bucket, never one shared bucket.
	state := e.keys.State(tenant, namespace, rule.ID, rule.Version)
	return limiter.Request{
		Rule:        rule,
		StateKey:    state,
		SeqKey:      e.keys.Seq(state),
		AdaptiveKey: e.keys.Adaptive(tenant, namespace),
		Cost:        cost,
		Peek:        peek,
	}
}

func (e *Evaluator) call(ctx context.Context, op string, req limiter.Request) (limiter.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.StoreTimeout)
	defer cancel()
	start := time.Now()
	res, err := e.limiter.Evaluate(ctx, req)
	e.metrics.ObserveStore(op, time.Since(start), err)
	return res, err
}

// warnSampled logs at most once per second so a store outage cannot flood logs.
func (e *Evaluator) warnSampled(msg string, args ...any) {
	now := time.Now().Unix()
	last := e.lastWarn.Load()
	if now > last && e.lastWarn.CompareAndSwap(last, now) {
		e.log.Warn(msg, args...)
	}
}
