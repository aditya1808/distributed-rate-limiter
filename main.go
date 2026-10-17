package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/aditya1808/distributed-rate-limiter/config"
	"github.com/aditya1808/distributed-rate-limiter/core"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	// Used as the container health check: the distroless image has no curl.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	if err := run(); err != nil {
		slog.Error("[MAIN] fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := newLogger(cfg.LogLevel).With("instance", cfg.InstanceID)
	slog.SetDefault(log)
	gin.SetMode(gin.ReleaseMode)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	startCtx, cancelStart := context.WithTimeout(ctx, 30*time.Second)
	app, err := core.New(startCtx, cfg, log)
	cancelStart()
	if err != nil {
		return err
	}
	defer func() {
		if err := app.Close(); err != nil {
			log.Warn("[MAIN] close", "error", err)
		}
	}()

	public := newServer(":"+cfg.Port, app.Public)
	internal := newServer(":"+cfg.InternalPort, app.Internal)

	errCh := make(chan error, 2)
	for _, srv := range []*http.Server{public, internal} {
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("listen %s: %w", srv.Addr, err)
			}
		}()
	}
	log.Info("[MAIN] started", "version", version, "public_addr", public.Addr, "internal_addr", internal.Addr)

	select {
	case <-ctx.Done():
	case err := <-errCh:
		return err
	}

	// Fail readiness first so the load balancer stops routing here, then
	// drain in-flight requests.
	log.Info("[MAIN] shutting down", "drain_delay", cfg.ShutdownDrainDelay)
	app.ShuttingDown.Store(true)
	time.Sleep(cfg.ShutdownDrainDelay)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	return errors.Join(public.Shutdown(shutdownCtx), internal.Shutdown(shutdownCtx))
}

func newServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

func healthcheck() int {
	port := 8080
	if v := os.Getenv("PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil || p < 1 || p > 65535 {
			return 1
		}
		port = p
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	//nolint:gosec // loopback only, port is numeric
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/readyz", port), nil)
	if err != nil {
		return 1
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // same request as above
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
