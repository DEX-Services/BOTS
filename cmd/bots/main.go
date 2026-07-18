// Command bots runs the trading-bots service: a standalone HTTP API that lets
// authenticated users create, run, stop, and copy bot strategies, executing
// them against the matching engine on the user's behalf.
//
// The bots service shares Dex-Backend's JWT secret (so it can verify the
// dex_session cookie) and the same Postgres instance (so bot configs/state
// live next to the ledger). It never touches the matching engine's hot path;
// it is a pure client of the engine's public /order and /cancel endpoints.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dex/bots/internal/api"
	"github.com/dex/bots/internal/auth"
	"github.com/dex/bots/internal/config"
	"github.com/dex/bots/internal/engine"
	"github.com/dex/bots/internal/marketdata"
	"github.com/dex/bots/internal/runtime"
	"github.com/dex/bots/internal/store"
	"github.com/joho/godotenv"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	if err := godotenv.Load(); err != nil {
		slog.Info("no .env file, using env vars")
	}

	cfg, err := config.Load()
	if err != nil {
		slog.Error("config invalid", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	st, err := store.New(ctx, cfg.PostgresURI)
	if err != nil {
		slog.Error("postgres unavailable; bots service requires the DB", "error", err)
		os.Exit(1)
	}
	slog.Info("postgres connected")

	engineClient := engine.NewClient(cfg.EngineURL, cfg.EngineConcurrency)
	hub := marketdata.NewHub(engineClient, cfg.MarketDataPoll())
	manager := runtime.NewManager(engineClient, hub, st)

	verifier := auth.NewVerifier(cfg.JWTSecret)
	server := api.NewServer(st, manager, verifier)
	handler := api.CORS(cfg.AllowedOrigins, server.Routes())

	// Resume bots that were running before a restart.
	manager.StartAll(ctx)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	go func() {
		slog.Info("bots service listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down; stopping bots")
	manager.StopAll()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)
	slog.Info("shutdown complete")
}
