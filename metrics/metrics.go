package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// Metrics owns a private registry so several app instances can coexist in
// one process (the multi-instance tests rely on this).
type Metrics struct {
	registry *prometheus.Registry

	HTTPRequests  *prometheus.CounterVec
	HTTPDuration  *prometheus.HistogramVec
	Decisions     *prometheus.CounterVec
	StoreErrors   *prometheus.CounterVec
	StoreDuration *prometheus.HistogramVec
	RuleCacheSize prometheus.Gauge
	RuleReloads   *prometheus.CounterVec
	RuleMisses    *prometheus.CounterVec
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests by route template, method and status.",
		}, []string{"route", "method", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency by route template and method.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"route", "method"}),
		Decisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimit_decisions_total",
			Help: "Quota decisions. Labelled by rule namespace, never tenant, to bound cardinality.",
		}, []string{"namespace", "algorithm", "result", "reason"}),
		StoreErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimit_store_errors_total",
			Help: "State store errors by operation.",
		}, []string{"op"}),
		StoreDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ratelimit_store_duration_seconds",
			Help:    "State store call latency by operation.",
			Buckets: []float64{.0002, .0005, .001, .0025, .005, .01, .025, .05, .1},
		}, []string{"op"}),
		RuleCacheSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ratelimit_rule_cache_assignments",
			Help: "Assignments (with their rules) held in this instance's cache.",
		}),
		RuleReloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimit_rule_cache_updates_total",
			Help: "Full rule cache reloads by trigger (event, periodic, write).",
		}, []string{"source"}),
		RuleMisses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimit_rule_cache_misses_total",
			Help: "Lookups for pairs absent from the snapshot, by where they were answered (local, redis, postgres, error).",
		}, []string{"source"}),
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.HTTPRequests, m.HTTPDuration, m.Decisions, m.StoreErrors,
		m.StoreDuration, m.RuleCacheSize, m.RuleReloads, m.RuleMisses,
	)
	return m
}

// RegisterRedisPool exposes connection pool state. Misses mean a connection
// was dialed on demand; timeouts mean a caller waited for a free connection.
func (m *Metrics) RegisterRedisPool(stats func() *redis.PoolStats) {
	counter := func(name, help string, v func(*redis.PoolStats) uint32) prometheus.Collector {
		return prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help},
			func() float64 { return float64(v(stats())) })
	}
	gauge := func(name, help string, v func(*redis.PoolStats) uint32) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help},
			func() float64 { return float64(v(stats())) })
	}
	m.registry.MustRegister(
		counter("redis_pool_hits_total", "Connections reused from the pool.", func(s *redis.PoolStats) uint32 { return s.Hits }),
		counter("redis_pool_misses_total", "Connections dialed on demand because none was idle.", func(s *redis.PoolStats) uint32 { return s.Misses }),
		counter("redis_pool_timeouts_total", "Waits for a free connection that timed out.", func(s *redis.PoolStats) uint32 { return s.Timeouts }),
		gauge("redis_pool_connections", "Open connections.", func(s *redis.PoolStats) uint32 { return s.TotalConns }),
		gauge("redis_pool_idle_connections", "Idle connections.", func(s *redis.PoolStats) uint32 { return s.IdleConns }),
	)
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}

func (m *Metrics) ObserveStore(op string, d time.Duration, err error) {
	m.StoreDuration.WithLabelValues(op).Observe(d.Seconds())
	if err != nil {
		m.StoreErrors.WithLabelValues(op).Inc()
	}
}

func (m *Metrics) RecordDecision(namespace, algorithm string, allowed bool, reason string) {
	result := "denied"
	if allowed {
		result = "allowed"
	}
	m.Decisions.WithLabelValues(namespace, algorithm, result, reason).Inc()
}
