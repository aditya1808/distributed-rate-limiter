package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port         string
	InternalPort string

	RedisURL      string
	RedisTimeout  time.Duration
	RedisPoolSize int
	// RedisMinIdleConns keeps connections dialed ahead of demand: a cold dial
	// (TLS to a managed store) inside REDIS_TIMEOUT turns into a degraded decision.
	RedisMinIdleConns int
	KeyPrefix         string

	PostgresURL string
	// PostgresTimeout bounds a rule lookup on the hot path (cache miss only).
	PostgresTimeout  time.Duration
	PostgresMaxConns int

	RuleRefreshInterval time.Duration
	// RuleMissTTL is how long an answer for a pair missing from the snapshot
	// (including "no rule") is reused, locally and in Redis.
	RuleMissTTL  time.Duration
	DefaultAllow bool

	InstanceID       string
	ExposeInstanceID bool
	LogLevel         string

	ShutdownDrainDelay time.Duration
	ShutdownTimeout    time.Duration
}

func Load() (Config, error) {
	host, _ := os.Hostname()
	var errs []error
	cfg := Config{
		Port:                str("PORT", "8080"),
		InternalPort:        str("INTERNAL_PORT", "9090"),
		RedisURL:            str("REDIS_URL", "redis://localhost:6379/0"),
		RedisTimeout:        dur("REDIS_TIMEOUT", 50*time.Millisecond, &errs),
		RedisPoolSize:       num("REDIS_POOL_SIZE", 50, &errs),
		RedisMinIdleConns:   num("REDIS_MIN_IDLE_CONNS", -1, &errs),
		KeyPrefix:           str("KEY_PREFIX", "rl"),
		PostgresURL:         str("POSTGRES_URL", "postgres://postgres:postgres@localhost:5432/ratelimiter?sslmode=disable"),
		PostgresTimeout:     dur("POSTGRES_TIMEOUT", 200*time.Millisecond, &errs),
		PostgresMaxConns:    num("POSTGRES_MAX_CONNS", 10, &errs),
		RuleRefreshInterval: dur("RULE_REFRESH_INTERVAL", 5*time.Second, &errs),
		RuleMissTTL:         dur("RULE_MISS_TTL", 5*time.Second, &errs),
		InstanceID:          str("INSTANCE_ID", host),
		ExposeInstanceID:    boolean("EXPOSE_INSTANCE_ID", true, &errs),
		LogLevel:            str("LOG_LEVEL", "info"),
		ShutdownDrainDelay:  dur("SHUTDOWN_DRAIN_DELAY", 5*time.Second, &errs),
		ShutdownTimeout:     dur("SHUTDOWN_TIMEOUT", 15*time.Second, &errs),
	}
	switch policy := strings.ToLower(str("DEFAULT_POLICY", "allow")); policy {
	case "allow":
		cfg.DefaultAllow = true
	case "deny":
		cfg.DefaultAllow = false
	default:
		errs = append(errs, fmt.Errorf("DEFAULT_POLICY must be allow or deny, got %q", policy))
	}
	if cfg.RedisMinIdleConns == -1 {
		cfg.RedisMinIdleConns = cfg.RedisPoolSize
	}
	if cfg.RedisPoolSize < 1 {
		errs = append(errs, errors.New("REDIS_POOL_SIZE must be at least 1"))
	}
	if cfg.RedisMinIdleConns < 0 || cfg.RedisMinIdleConns > cfg.RedisPoolSize {
		errs = append(errs, errors.New("REDIS_MIN_IDLE_CONNS must be between 0 and REDIS_POOL_SIZE"))
	}
	if cfg.RedisTimeout <= 0 {
		errs = append(errs, errors.New("REDIS_TIMEOUT must be positive"))
	}
	if cfg.RuleRefreshInterval <= 0 {
		errs = append(errs, errors.New("RULE_REFRESH_INTERVAL must be positive"))
	}
	if cfg.RuleMissTTL <= 0 {
		errs = append(errs, errors.New("RULE_MISS_TTL must be positive"))
	}
	if cfg.PostgresTimeout <= 0 {
		errs = append(errs, errors.New("POSTGRES_TIMEOUT must be positive"))
	}
	if cfg.PostgresMaxConns < 1 {
		errs = append(errs, errors.New("POSTGRES_MAX_CONNS must be at least 1"))
	}
	return cfg, errors.Join(errs...)
}

func str(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func dur(key string, def time.Duration, errs *[]error) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return d
}

func num(key string, def int, errs *[]error) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return n
}

func boolean(key string, def bool, errs *[]error) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return b
}
