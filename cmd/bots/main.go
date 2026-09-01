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
	"github.com/dex/bots/internal/backend"
	"github.com/dex/bots/internal/config"
	"github.com/dex/bots/internal/engine"
	"github.com/dex/bots/internal/index"
	"github.com/dex/bots/internal/marketdata"
	"github.com/dex/bots/internal/mm"
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
	engineClient.SetEngineSecret(cfg.EngineSecret)
	backendClient := backend.NewClient(cfg.BackendURL, cfg.EngineSecret)
	hub := marketdata.NewHub(engineClient, cfg.MarketDataPoll())

	// Index reader for market-maker bots. Optional: if REDIS_SERVICE_URI is
	// unset or unreachable, MM bots receive a stale snapshot and refuse to
	// quote, but every other strategy runs unaffected.
	var idx *index.Reader
	if cfg.RedisURI != "" {
		// Aiven Redis can take a few seconds to accept a connection just after
		// a local stack restart. Retry before declaring MM market data absent;
		// otherwise the recovery path used to abort the entire bots service.
		for attempt := 1; attempt <= 3; attempt++ {
			idx, err = index.New(ctx, cfg.RedisURI, cfg.IndexPrefix, time.Duration(cfg.IndexMaxAgeMs)*time.Millisecond)
			if err == nil {
				break
			}
			slog.Warn("index price reader connect failed", "attempt", attempt, "error", err)
			if attempt < 3 {
				time.Sleep(2 * time.Second)
			}
		}
		if err != nil || idx == nil {
			slog.Warn("index price reader unavailable; market-maker bots will not quote", "error", err)
			idx = nil
		} else {
			slog.Info("index price reader connected", "prefix", cfg.IndexPrefix)
		}
	} else {
		slog.Warn("REDIS_SERVICE_URI unset; market-maker bots will not quote")
	}

	manager := runtime.NewManager(engineClient, hub, st, idx)

	// Market-maker restart recovery runs before the matching engine starts (see
	// run.sh). It clears the durable Postgres locks orphaned by the engine's
	// restart (the engine's own live book is gone, but nothing tells Postgres
	// that) and re-syncs each desk's investment budget — it never touches
	// actual balances, which the engine's own startup backfill already primes
	// correctly from the same Postgres row (see mm.Service.Recredit's doc).
	//
	// StartAll below unconditionally resumes any bot that was running before
	// the restart, without going through the SetEnabled path that would
	// otherwise retry Recredit on its own — so a desk resumed here can be left
	// with stale locks if Recredit hasn't finished (e.g. a transient backend
	// connection hiccup at boot) before it starts. Retry with a short backoff
	// so a slow-to-connect dependency doesn't strand a desk in that state;
	// only fall through with a warning (rather than block startup
	// indefinitely) if it genuinely never comes up.
	mmSvc := mm.NewService(st, engineClient, backendClient, manager)
	recreditErr := mmSvc.Recredit(ctx)
	for attempt := 0; recreditErr != nil && attempt < 5; attempt++ {
		slog.Warn("market-maker recredit failed, retrying", "attempt", attempt+1, "error", recreditErr)
		time.Sleep(time.Duration(attempt+1) * time.Second)
		recreditErr = mmSvc.Recredit(ctx)
	}
	if recreditErr != nil {
		slog.Warn("market-maker recovery deferred; resumed spot desks may quote against stale balances until manually restarted", "error", recreditErr)
	} else {
		slog.Info("market-maker balances recovered")
	}

	verifier := auth.NewVerifier(cfg.JWTSecret)
	server := api.NewServer(st, manager, verifier, mmSvc)
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
