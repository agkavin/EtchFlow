package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcusferl/etchflow/internal/api"
	"github.com/marcusferl/etchflow/internal/api/handler"
	"github.com/marcusferl/etchflow/internal/config"
	"github.com/marcusferl/etchflow/internal/store"
	"github.com/marcusferl/etchflow/internal/worker"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// ── 1. Load configuration ─────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}

	// ── 2. Initialise logger ──────────────────────────────────────────────
	logger, err := buildLogger(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: failed to build logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	logger.Info("EtchFlow starting",
		zap.String("version", "0.1.0-mvp"),
		zap.Int("port", cfg.HTTPPort),
		zap.String("log_level", cfg.LogLevel),
	)

	// ── 3. Connect to PostgreSQL ──────────────────────────────────────────
	ctx := context.Background()
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		logger.Fatal("invalid DATABASE_URL", zap.Error(err))
	}
	poolConfig.MaxConns = int32(cfg.DBPoolMaxConns)

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		logger.Fatal("failed to create connection pool", zap.Error(err))
	}
	defer pool.Close()

	// Verify database is reachable before accepting traffic
	if err := pool.Ping(ctx); err != nil {
		logger.Fatal("database ping failed — is Postgres running?", zap.Error(err))
	}
	logger.Info("database connected", zap.Int32("max_conns", poolConfig.MaxConns))

	// ── 4. Initialise store layer ─────────────────────────────────────────
	s := store.New(pool)

	// ── 5. Initialise handlers and router ─────────────────────────────────
	h := handler.New(s, logger)
	router := api.NewRouter(h, logger)

	// ── 5.5 Start background workers ──────────────────────────────────────
	reaper := worker.NewReaper(s, logger, 10*time.Second, 60*time.Second)
	reaper.Start(ctx)
	defer reaper.Stop()

	retryScanner := worker.NewRetryScanner(s, logger, 5*time.Second)
	retryScanner.Start(ctx)
	defer retryScanner.Stop()

	// ── 6. Start HTTP server ──────────────────────────────────────────────
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 35 * time.Second, // Slightly > chi Timeout middleware (30s)
		IdleTimeout:  120 * time.Second,
	}

	// Start server in background goroutine
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("server listening", zap.String("addr", srv.Addr))
		serverErrors <- srv.ListenAndServe()
	}()

	// ── 7. Graceful shutdown ──────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", zap.Error(err))
		}
	case sig := <-quit:
		logger.Info("shutdown signal received", zap.String("signal", sig.String()))

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful shutdown failed", zap.Error(err))
		} else {
			logger.Info("server shut down gracefully")
		}
	}
}

// buildLogger creates a zap logger based on the configured level and format.
func buildLogger(level, format string) (*zap.Logger, error) {
	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		zapLevel = zapcore.InfoLevel
	}

	var zapCfg zap.Config
	if format == "json" {
		zapCfg = zap.NewProductionConfig()
	} else {
		zapCfg = zap.NewDevelopmentConfig()
		zapCfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}
	zapCfg.Level = zap.NewAtomicLevelAt(zapLevel)

	return zapCfg.Build()
}
