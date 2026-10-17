package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
			Name: "ratelimit_rule_cache_rules",
			Help: "Rules currently held in this instance's cache.",
		}),
		RuleReloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimit_rule_cache_updates_total",
			Help: "Rule cache updates by source (event, reload, local).",
		}, []string{"source"}),
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.HTTPRequests, m.HTTPDuration, m.Decisions, m.StoreErrors,
		m.StoreDuration, m.RuleCacheSize, m.RuleReloads,
	)
	return m
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
