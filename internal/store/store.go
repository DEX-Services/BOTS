// Package store is the bots-service Postgres layer. It owns the `bots` table
// (config, status, state, stats) and reuses the same Postgres instance as
// Dex-Backend and the matching engine. State and stats are JSONB so strategy
// state can evolve without schema churn.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dex/bots/internal/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a bot id does not exist.
var ErrNotFound = errors.New("bot not found")

// Store wraps a pgx connection pool for bot persistence.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a pool and runs idempotent migrations.
func New(ctx context.Context, uri string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}
	cfg.MaxConns = 10
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schema)
	return err
}

const schema = `
CREATE TABLE IF NOT EXISTS bots (
    id             TEXT PRIMARY KEY,
    user_id        TEXT        NOT NULL,
    wallet_address TEXT        NOT NULL,
    name           TEXT        NOT NULL,
    strategy       TEXT        NOT NULL,
    market         TEXT        NOT NULL,
    symbol         TEXT        NOT NULL,
    investment     TEXT        NOT NULL DEFAULT '0',
    config         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    is_public      BOOLEAN     NOT NULL DEFAULT FALSE,
    status         TEXT        NOT NULL DEFAULT 'draft',
    state          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    stats          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    error          TEXT        NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at     TIMESTAMPTZ,
    stopped_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS bots_user_idx   ON bots(user_id);
CREATE INDEX IF NOT EXISTS bots_public_idx ON bots(is_public) WHERE is_public;
CREATE INDEX IF NOT EXISTS bots_status_idx ON bots(status);
`

// Create inserts a new bot row.
func (s *Store) Create(ctx context.Context, bot *models.Bot) error {
	if bot.ID == "" {
		bot.ID = uuid.NewString()
	}
	now := time.Now()
	bot.CreatedAt = now
	bot.UpdatedAt = now
	if bot.Status == "" {
		bot.Status = models.StatusDraft
	}
	if bot.State == nil {
		bot.State = map[string]any{}
	}
	if bot.Config == nil {
		bot.Config = map[string]string{}
	}
	stats, _ := json.Marshal(models.NewStats())
	config, _ := json.Marshal(bot.Config)
	state, _ := json.Marshal(bot.State)
	_, err := s.pool.Exec(ctx, `
INSERT INTO bots (id, user_id, wallet_address, name, strategy, market, symbol, investment, config, is_public, status, state, stats, error, created_at, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
		bot.ID, bot.UserID, bot.WalletAddress, bot.Name, bot.Strategy,
		string(bot.Market), bot.Symbol, bot.Investment, config, bot.IsPublic,
		string(bot.Status), state, stats, bot.Error, bot.CreatedAt, bot.UpdatedAt,
	)
	return err
}

// Get fetches a single bot by id.
func (s *Store) Get(ctx context.Context, id string) (*models.Bot, error) {
	row := s.pool.QueryRow(ctx, `
SELECT id, user_id, wallet_address, name, strategy, market, symbol, investment, config, is_public, status, state, stats, error, created_at, updated_at, started_at, stopped_at
FROM bots WHERE id = $1`, id)
	b, err := scanBot(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return b, err
}

// ListByUser returns all bots owned by a user.
func (s *Store) ListByUser(ctx context.Context, userID string) ([]models.Bot, error) {
	rows, err := s.pool.Query(ctx, `
SELECT id, user_id, wallet_address, name, strategy, market, symbol, investment, config, is_public, status, state, stats, error, created_at, updated_at, started_at, stopped_at
FROM bots WHERE user_id = $1 ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	return scanBots(rows)
}

// ListRunning returns all bots that should be active on startup.
func (s *Store) ListRunning(ctx context.Context) ([]models.Bot, error) {
	rows, err := s.pool.Query(ctx, `
SELECT id, user_id, wallet_address, name, strategy, market, symbol, investment, config, is_public, status, state, stats, error, created_at, updated_at, started_at, stopped_at
FROM bots WHERE status = $1`, string(models.StatusRunning))
	if err != nil {
		return nil, err
	}
	return scanBots(rows)
}

// ListPublic returns public bots for the marketplace, optionally filtered.
func (s *Store) ListPublic(ctx context.Context, strategy, market string) ([]models.Bot, error) {
	q := `SELECT id, user_id, wallet_address, name, strategy, market, symbol, investment, config, is_public, status, state, stats, error, created_at, updated_at, started_at, stopped_at
FROM bots WHERE is_public = TRUE AND status IN ('running','stopped')`
	args := []any{}
	if strategy != "" {
		args = append(args, strategy)
		q += fmt.Sprintf(" AND strategy = $%d", len(args))
	}
	if market != "" {
		args = append(args, market)
		q += fmt.Sprintf(" AND market = $%d", len(args))
	}
	q += " ORDER BY created_at DESC LIMIT 100"
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanBots(rows)
}

// UpdateStatus sets status (and optional error) and touches updated_at.
func (s *Store) UpdateStatus(ctx context.Context, id string, status models.Status, errMsg string) error {
	_, err := s.pool.Exec(ctx, `UPDATE bots SET status=$2, error=$3, updated_at=now() WHERE id=$1`,
		id, string(status), errMsg)
	return err
}

// MarkRunning sets status=running and stamps started_at.
func (s *Store) MarkRunning(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE bots SET status='running', error='', started_at=now(), updated_at=now() WHERE id=$1`, id)
	return err
}

// MarkStopped sets status=stopped and stamps stopped_at.
func (s *Store) MarkStopped(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE bots SET status='stopped', stopped_at=now(), updated_at=now() WHERE id=$1`, id)
	return err
}

// SaveState persists the runtime state and computed stats for a bot.
func (s *Store) SaveState(ctx context.Context, id string, state any, stats models.Stats) error {
	sb, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal bot state: %w", err)
	}
	stb, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("marshal bot stats: %w", err)
	}
	_, err = s.pool.Exec(ctx, `UPDATE bots SET state=$2, stats=$3, updated_at=now() WHERE id=$1`, id, sb, stb)
	return err
}

// Delete removes a bot row.
func (s *Store) Delete(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM bots WHERE id=$1`, id)
	return err
}

type scanner interface {
	Scan(dest ...any) error
}

type rowScanner interface {
	Next() bool
	Scan(dest ...any) error
	Close()
	Err() error
}

func scanBot(row scanner) (*models.Bot, error) {
	var b models.Bot
	var config, state, stats []byte
	var startedAt, stoppedAt *time.Time
	var market string
	err := row.Scan(
		&b.ID, &b.UserID, &b.WalletAddress, &b.Name, &b.Strategy, &market, &b.Symbol,
		&b.Investment, &config, &b.IsPublic, &b.Status, &state, &stats, &b.Error,
		&b.CreatedAt, &b.UpdatedAt, &startedAt, &stoppedAt,
	)
	if err != nil {
		return nil, err
	}
	b.Market = models.Market(market)
	b.StartedAt = startedAt
	b.StoppedAt = stoppedAt
	// Corrupt persisted JSON must surface, not silently become defaults: a bot
	// resumed with an empty state would re-place orders it already placed.
	if len(config) > 0 {
		if err := json.Unmarshal(config, &b.Config); err != nil {
			return nil, fmt.Errorf("bot %s: corrupt config JSON: %w", b.ID, err)
		}
	}
	if b.Config == nil {
		b.Config = map[string]string{}
	}
	if len(state) > 0 {
		if err := json.Unmarshal(state, &b.State); err != nil {
			return nil, fmt.Errorf("bot %s: corrupt state JSON: %w", b.ID, err)
		}
	}
	if len(stats) > 0 {
		if err := json.Unmarshal(stats, &b.Stats); err != nil {
			return nil, fmt.Errorf("bot %s: corrupt stats JSON: %w", b.ID, err)
		}
	}
	if b.State == nil {
		b.State = map[string]any{}
	}
	return &b, nil
}

func scanBots(rows rowScanner) ([]models.Bot, error) {
	defer rows.Close()
	// Non-nil so the JSON API serializes "bots":[] rather than "bots":null on an
	// empty result; the frontend indexes .bots.length directly.
	out := []models.Bot{}
	for rows.Next() {
		b, err := scanBot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *b)
	}
	return out, rows.Err()
}
