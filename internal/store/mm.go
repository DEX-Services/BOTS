// mm.go is the market-maker persistence layer: the market_makers desks table
// and the append-only mm_funding_ledger audit table. Funding operations mutate
// base_amount/quote_amount and write an audit row in one transaction so the
// balance and its history never diverge.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dex/bots/internal/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// ErrMMNotFound is returned when a market-maker id does not exist.
var ErrMMNotFound = errors.New("market maker not found")

// ErrInsufficientFunds is returned when a withdraw exceeds available capital.
var ErrInsufficientFunds = errors.New("insufficient allocated funds")

const mmSchema = `
CREATE TABLE IF NOT EXISTS market_makers (
    id             TEXT PRIMARY KEY,
    base           TEXT        NOT NULL,
    market         TEXT        NOT NULL,
    symbol         TEXT        NOT NULL,
    wallet_address TEXT        NOT NULL UNIQUE,
    bot_id         TEXT        NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
    base_amount    TEXT        NOT NULL DEFAULT '0',
    quote_amount   TEXT        NOT NULL DEFAULT '0',
    enabled        BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (base, market)
);
-- Migrate desks created before base/quote were tracked separately: the old
-- allocated_usdc column held quote-side capital only (base inventory was
-- synthesized by a 50/50 split at recredit time, which no longer happens).
ALTER TABLE market_makers ADD COLUMN IF NOT EXISTS base_amount TEXT NOT NULL DEFAULT '0';
ALTER TABLE market_makers ADD COLUMN IF NOT EXISTS quote_amount TEXT NOT NULL DEFAULT '0';
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'market_makers' AND column_name = 'allocated_usdc') THEN
        UPDATE market_makers SET quote_amount = allocated_usdc WHERE quote_amount = '0';
        ALTER TABLE market_makers DROP COLUMN allocated_usdc;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS mm_funding_ledger (
    id              TEXT PRIMARY KEY,
    market_maker_id TEXT        NOT NULL REFERENCES market_makers(id) ON DELETE CASCADE,
    asset           TEXT        NOT NULL DEFAULT 'quote',
    direction       TEXT        NOT NULL,
    amount          TEXT        NOT NULL,
    balance_after   TEXT        NOT NULL,
    admin_id        TEXT        NOT NULL DEFAULT '',
    note            TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS mm_funding_mm_idx ON mm_funding_ledger(market_maker_id);
ALTER TABLE mm_funding_ledger ADD COLUMN IF NOT EXISTS admin_id TEXT NOT NULL DEFAULT '';
ALTER TABLE mm_funding_ledger ADD COLUMN IF NOT EXISTS asset TEXT NOT NULL DEFAULT 'quote';
`

func (s *Store) migrateMM(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, mmSchema)
	return err
}

// CreateMM inserts a market-maker desk row.
func (s *Store) CreateMM(ctx context.Context, mm *models.MarketMaker) error {
	if mm.ID == "" {
		mm.ID = uuid.NewString()
	}
	now := time.Now()
	mm.CreatedAt = now
	mm.UpdatedAt = now
	if mm.BaseAmount == "" {
		mm.BaseAmount = "0"
	}
	if mm.QuoteAmount == "" {
		mm.QuoteAmount = "0"
	}
	_, err := s.pool.Exec(ctx, `
INSERT INTO market_makers (id, base, market, symbol, wallet_address, bot_id, base_amount, quote_amount, enabled, created_at, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		mm.ID, mm.Base, string(mm.Market), mm.Symbol, mm.WalletAddress, mm.BotID,
		mm.BaseAmount, mm.QuoteAmount, mm.Enabled, mm.CreatedAt, mm.UpdatedAt,
	)
	return err
}

// GetMM fetches a desk by id.
func (s *Store) GetMM(ctx context.Context, id string) (*models.MarketMaker, error) {
	row := s.pool.QueryRow(ctx, mmSelect+` WHERE id = $1`, id)
	mm, err := scanMM(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrMMNotFound
	}
	return mm, err
}

// DeleteMM removes a desk row. Funding-ledger rows are left as an audit trail;
// the caller is responsible for the underlying bot.
func (s *Store) DeleteMM(ctx context.Context, id string) error {
	ct, err := s.pool.Exec(ctx, `DELETE FROM market_makers WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrMMNotFound
	}
	return nil
}

// EnabledDesk is one enabled market-maker desk and the current status of the
// bot behind it, as needed to decide whether a restart should resume it.
type EnabledDesk struct {
	BotID  string
	Symbol string
	Status string
}

// EnabledDesks returns every desk whose enabled flag is set, with its bot's
// status, regardless of what that status is.
//
// `enabled` on the desk is the admin's standing intent ("this desk should be
// quoting"); bots.status is merely where the worker happened to be when the
// process last exited. Those two diverge on every shutdown: StopAll marks each
// worker it stops cleanly as stopped, so resuming from status alone silently
// drops exactly the desks that shut down properly — they stay dark until an
// admin re-toggles them by hand, with nothing logged. Startup reads intent from
// here instead, and uses Status only to leave genuinely failed desks alone.
// Ordered so the resume sequence is deterministic.
func (s *Store) EnabledDesks(ctx context.Context) ([]EnabledDesk, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT mm.bot_id, mm.symbol, b.status
		FROM market_makers mm
		JOIN bots b ON b.id = mm.bot_id
		WHERE mm.enabled = true
		ORDER BY mm.created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []EnabledDesk{}
	for rows.Next() {
		var d EnabledDesk
		if err := rows.Scan(&d.BotID, &d.Symbol, &d.Status); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
func (s *Store) ListMM(ctx context.Context) ([]models.MarketMaker, error) {
	rows, err := s.pool.Query(ctx, mmSelect+` ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.MarketMaker{}
	for rows.Next() {
		mm, err := scanMM(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *mm)
	}
	return out, rows.Err()
}

// SetMMEnabled flips the admin start/stop flag.
func (s *Store) SetMMEnabled(ctx context.Context, id string, enabled bool) error {
	ct, err := s.pool.Exec(ctx, `UPDATE market_makers SET enabled=$2, updated_at=now() WHERE id=$1`, id, enabled)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrMMNotFound
	}
	return nil
}

// mmAssetColumns maps the logical leg name ("base" or "quote") to its column.
var mmAssetColumns = map[string]string{
	"base":  "base_amount",
	"quote": "quote_amount",
}

// Fund applies a funding change to one leg (asset: "base" or "quote") of a
// desk atomically: it re-reads that leg's amount under a row lock, computes
// the new balance, guards a withdraw against available capital (caller
// supplies availableFloor — the balance below which a withdraw is rejected,
// typically the amount reserved behind resting orders for that asset),
// updates the column, and appends an audit row. Returns the new balance.
// direction is "deposit" or "withdraw"; amount must be positive.
func (s *Store) Fund(ctx context.Context, id, asset, direction string, amount decimal.Decimal, availableFloor decimal.Decimal, adminID, note string) (decimal.Decimal, error) {
	column, ok := mmAssetColumns[asset]
	if !ok {
		return decimal.Zero, fmt.Errorf("asset must be %q or %q", "base", "quote")
	}
	if !amount.IsPositive() {
		return decimal.Zero, fmt.Errorf("amount must be positive")
	}
	if direction != "deposit" && direction != "withdraw" {
		return decimal.Zero, fmt.Errorf("direction must be deposit or withdraw")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return decimal.Zero, err
	}
	defer tx.Rollback(ctx)

	var curStr string
	err = tx.QueryRow(ctx, `SELECT `+column+` FROM market_makers WHERE id=$1 FOR UPDATE`, id).Scan(&curStr)
	if errors.Is(err, pgx.ErrNoRows) {
		return decimal.Zero, ErrMMNotFound
	}
	if err != nil {
		return decimal.Zero, err
	}
	cur, err := decimal.NewFromString(curStr)
	if err != nil {
		return decimal.Zero, fmt.Errorf("corrupt %s %q: %w", column, curStr, err)
	}

	var next decimal.Decimal
	if direction == "deposit" {
		next = cur.Add(amount)
	} else {
		next = cur.Sub(amount)
		// Withdraw cannot pull below capital reserved behind live orders.
		if next.LessThan(availableFloor) {
			return decimal.Zero, ErrInsufficientFunds
		}
	}

	if _, err := tx.Exec(ctx, `UPDATE market_makers SET `+column+`=$2, updated_at=now() WHERE id=$1`, id, next.String()); err != nil {
		return decimal.Zero, err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO mm_funding_ledger (id, market_maker_id, asset, direction, amount, balance_after, admin_id, note, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,now())`,
		uuid.NewString(), id, asset, direction, amount.String(), next.String(), adminID, note); err != nil {
		return decimal.Zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return decimal.Zero, err
	}
	return next, nil
}

// FundingHistory returns the audit rows for a desk, newest first.
func (s *Store) FundingHistory(ctx context.Context, id string) ([]models.MMFundingEntry, error) {
	rows, err := s.pool.Query(ctx, `
SELECT id, market_maker_id, asset, direction, amount, balance_after, admin_id, note, created_at
FROM mm_funding_ledger WHERE market_maker_id=$1 ORDER BY created_at DESC`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.MMFundingEntry{}
	for rows.Next() {
		var e models.MMFundingEntry
		if err := rows.Scan(&e.ID, &e.MarketMakerID, &e.Asset, &e.Direction, &e.Amount, &e.BalanceAfter, &e.AdminID, &e.Note, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

const mmSelect = `SELECT id, base, market, symbol, wallet_address, bot_id, base_amount, quote_amount, enabled, created_at, updated_at FROM market_makers`

func scanMM(row scanner) (*models.MarketMaker, error) {
	var mm models.MarketMaker
	var market string
	err := row.Scan(&mm.ID, &mm.Base, &market, &mm.Symbol, &mm.WalletAddress,
		&mm.BotID, &mm.BaseAmount, &mm.QuoteAmount, &mm.Enabled, &mm.CreatedAt, &mm.UpdatedAt)
	if err != nil {
		return nil, err
	}
	mm.Market = models.Market(market)
	return &mm, nil
}
