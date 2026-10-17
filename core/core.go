package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/aditya1808/distributed-rate-limiter/cache"
	"github.com/aditya1808/distributed-rate-limiter/config"
	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/controllers/assignment"
	"github.com/aditya1808/distributed-rate-limiter/controllers/check"
	"github.com/aditya1808/distributed-rate-limiter/controllers/dashboard"
	"github.com/aditya1808/distributed-rate-limiter/controllers/feedback"
	"github.com/aditya1808/distributed-rate-limiter/controllers/health"
	"github.com/aditya1808/distributed-rate-limiter/controllers/quota"
	"github.com/aditya1808/distributed-rate-limiter/controllers/rule"
	"github.com/aditya1808/distributed-rate-limiter/limiter"
	"github.com/aditya1808/distributed-rate-limiter/manager"
	"github.com/aditya1808/distributed-rate-limiter/metrics"
	"github.com/aditya1808/distributed-rate-limiter/middlewares"
	"github.com/aditya1808/distributed-rate-limiter/persistence/pgstore"
	"github.com/aditya1808/distributed-rate-limiter/persistence/redisstore"
	"github.com/aditya1808/distributed-rate-limiter/routes"
	"github.com/aditya1808/distributed-rate-limiter/validations"
)

// App is a fully wired instance: routers plus the background workers they
// depend on.
type App struct {
	Config       config.Config
	Redis        *redis.Client
	Postgres     *pgxpool.Pool
	Metrics      *metrics.Metrics
	Public       *gin.Engine
	Internal     *gin.Engine
	ShuttingDown *atomic.Bool

	cancel   context.CancelFunc
	cache    *cache.RuleCache
	activity *manager.Activity
}

// New connects to Redis and Postgres, applies migrations, loads rules and
// wires every layer. ctx bounds
// startup only; background workers run until Close.
func New(ctx context.Context, cfg config.Config, log *slog.Logger) (*App, error) {
	if err := validations.Register(); err != nil {
		return nil, err
	}

	rdb, err := connectRedis(ctx, cfg, log)
	if err != nil {
		return nil, err
	}

	keys := constants.NewKeyspace(cfg.KeyPrefix)
	m := metrics.New()
	m.RegisterRedisPool(rdb.PoolStats)

	lim := limiter.NewRedisLimiter(rdb)
	if err := lim.Load(ctx); err != nil {
		_ = rdb.Close()
		return nil, err
	}

	pool, err := pgstore.Connect(ctx, cfg.PostgresURL, int32(cfg.PostgresMaxConns), log) // #nosec G115 -- validated small positive
	if err != nil {
		_ = rdb.Close()
		return nil, err
	}
	if err := pgstore.Migrate(ctx, pool, log); err != nil {
		pool.Close()
		_ = rdb.Close()
		return nil, err
	}

	ruleStore := pgstore.New(pool)
	bus := redisstore.NewChangeBus(rdb, keys)
	resolutions := redisstore.NewResolutionCache(rdb, keys)
	workerCtx, cancel := context.WithCancel(context.Background())

	lookup := cache.NewSharedLookup(resolutions, ruleStore, cfg.RuleMissTTL, cfg.RedisTimeout, cfg.PostgresTimeout)
	ruleCache := cache.New(ruleStore, bus, lookup, cache.Options{Refresh: cfg.RuleRefreshInterval, MissTTL: cfg.RuleMissTTL}, m, log)
	if err := ruleCache.Start(workerCtx); err != nil {
		cancel()
		pool.Close()
		_ = rdb.Close()
		return nil, err
	}

	activity := manager.NewActivity(redisstore.NewActivityStore(rdb, keys), log)
	go activity.Run(workerCtx)

	evaluator := manager.NewEvaluator(ruleCache, lim, activity, m, keys, log, manager.EvaluatorConfig{
		DefaultAllow: cfg.DefaultAllow,
		StoreTimeout: cfg.RedisTimeout,
	})
	ruleManager := manager.NewRuleManager(ruleStore, ruleCache, bus, resolutions, log)
	adaptiveManager := manager.NewAdaptiveManager(redisstore.NewAdaptiveStore(rdb, keys))

	shuttingDown := &atomic.Bool{}

	public := gin.New()
	middlewares.InitializeMiddlewares(public, middlewares.Options{
		Logger:           log,
		Metrics:          m,
		InstanceID:       cfg.InstanceID,
		ExposeInstanceID: cfg.ExposeInstanceID,
	})
	routes.AddRoutes(public, routes.Controllers{
		Check:      check.NewController(evaluator),
		Quota:      quota.NewController(evaluator),
		Rule:       rule.NewController(ruleManager),
		Assignment: assignment.NewController(ruleManager),
		Feedback:   feedback.NewController(adaptiveManager),
		Health:     health.NewController(func(ctx context.Context) error { return rdb.Ping(ctx).Err() }, ruleCache.Loaded, shuttingDown, cfg.InstanceID),
		Dashboard:  dashboard.NewController(activity, evaluator, cfg.InstanceID),
	})

	internal := gin.New()
	internal.Use(middlewares.Recovery(log))
	routes.AddInternalRoutes(internal, m.Handler())

	return &App{
		Config:       cfg,
		Redis:        rdb,
		Postgres:     pool,
		Metrics:      m,
		Public:       public,
		Internal:     internal,
		ShuttingDown: shuttingDown,
		cancel:       cancel,
		cache:        ruleCache,
		activity:     activity,
	}, nil
}

// Close stops background workers (flushing pending activity) and closes
// the store connections.
func (a *App) Close() error {
	a.cancel()
	<-a.cache.Done()
	<-a.activity.Done()
	a.Postgres.Close()
	return a.Redis.Close()
}

// warmPool dials n connections before the instance can report ready, so the
// first burst of traffic never pays for TLS dials inside REDIS_TIMEOUT.
// Connections are held simultaneously to force n distinct dials, then
// returned to the pool as idle.
func warmPool(ctx context.Context, rdb *redis.Client, n int, log *slog.Logger) {
	if n <= 0 {
		return
	}
	start := time.Now()
	conns := make([]*redis.Conn, 0, n)
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	for range n {
		c := rdb.Conn()
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := c.Ping(pingCtx).Err()
		cancel()
		if err != nil {
			_ = c.Close()
			log.Warn("[CORE] redis pool warm-up incomplete", "dialed", len(conns), "target", n, "error", err)
			return
		}
		conns = append(conns, c)
	}
	log.Info("[CORE] redis pool warmed", "connections", n, "duration_ms", time.Since(start).Milliseconds())
}

func connectRedis(ctx context.Context, cfg config.Config, log *slog.Logger) (*redis.Client, error) {
	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	opts.PoolSize = cfg.RedisPoolSize
	opts.MinIdleConns = cfg.RedisMinIdleConns
	// Never retire idle connections client-side: a retired connection is
	// re-dialed on the hot path. The store's own idle timeout must be off too.
	opts.ConnMaxIdleTime = -1
	opts.DialTimeout = 2 * time.Second
	opts.ReadTimeout = time.Second
	opts.WriteTimeout = time.Second
	opts.PoolTimeout = time.Second
	// Per-call deadlines come from the request context (REDIS_TIMEOUT).
	opts.ContextTimeoutEnabled = true

	rdb := redis.NewClient(opts)
	backoff := 250 * time.Millisecond
	for attempt := 1; ; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = rdb.Ping(pingCtx).Err()
		cancel()
		if err == nil {
			warmPool(ctx, rdb, cfg.RedisMinIdleConns, log)
			return rdb, nil
		}
		if attempt == 8 || errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled) {
			_ = rdb.Close()
			return nil, fmt.Errorf("connect to redis: %w", err)
		}
		log.Warn("[CORE] redis not reachable yet, retrying", "attempt", attempt, "error", err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
		}
		backoff = min(backoff*2, 4*time.Second)
	}
}
