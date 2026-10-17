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
	KeyPrefix     string

	RuleRefreshInterval time.Duration
	AdminToken          string
	DefaultAllow        bool

	InstanceID       string
	ExposeInstanceID bool
	LogLevel         string

	ShutdownDrainDelay time.Duration
	ShutdownTimeout    time.Duration
}

const minAdminTokenLen = 16

func Load() (Config, error) {
	host, _ := os.Hostname()
	var errs []error
	cfg := Config{
		Port:                str("PORT", "8080"),
		InternalPort:        str("INTERNAL_PORT", "9090"),
		RedisURL:            str("REDIS_URL", "redis://localhost:6379/0"),
		RedisTimeout:        dur("REDIS_TIMEOUT", 50*time.Millisecond, &errs),
		RedisPoolSize:       num("REDIS_POOL_SIZE", 50, &errs),
		KeyPrefix:           str("KEY_PREFIX", "rl"),
		RuleRefreshInterval: dur("RULE_REFRESH_INTERVAL", 5*time.Second, &errs),
		AdminToken:          os.Getenv("ADMIN_TOKEN"),
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
	if len(cfg.AdminToken) < minAdminTokenLen {
		errs = append(errs, fmt.Errorf("ADMIN_TOKEN must be set and at least %d characters", minAdminTokenLen))
	}
	if cfg.RedisTimeout <= 0 {
		errs = append(errs, errors.New("REDIS_TIMEOUT must be positive"))
	}
	if cfg.RuleRefreshInterval <= 0 {
		errs = append(errs, errors.New("RULE_REFRESH_INTERVAL must be positive"))
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
