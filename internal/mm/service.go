// Package mm is the market-maker funding and lifecycle service. It sits above
// the store and coordinates the invariant: a desk's base_amount/quote_amount
// (DB source of truth), the MM wallet's engine-ledger balances (in-memory,
// restart-wiped), and the underlying bot's tracked inventory must agree.
//
// Funding is admin-attested and per-leg: the admin moves real assets into the
// treasury wallet off-platform — the actual base asset (BTC, ETH, ...) AND the
// actual quote asset (USDB for spot, USDC for futures) — then records each amount here
// separately. Neither leg is derived from the other by any formula; a desk
// only ever holds what it was explicitly funded with. Deposits credit the
// engine ledger so the bot can quote against it; withdrawals debit it,
// guarded against capital reserved behind live orders.
package mm

import (
	"context"
	"fmt"
	"strings"

	"github.com/dex/bots/internal/backend"
	"github.com/dex/bots/internal/engine"
	"github.com/dex/bots/internal/models"
	"github.com/dex/bots/internal/runtime"
	"github.com/dex/bots/internal/store"
	"github.com/dex/bots/internal/strategy"
	"github.com/shopspring/decimal"
)

// collateralAsset returns the currency required by the desk's market.
// Spot markets use USDB; futures use USDC collateral. This is the desk's
// quote-leg asset; the base-leg asset is always desk.Base.
func collateralAsset(market models.Market) string {
	if market == models.Spot {
		return "USDB"
	}
	return "USDC"
}

// Service orchestrates market-maker desks: creation, funding, and start/stop.
type Service struct {
	store   *store.Store
	engine  *engine.Client
	backend *backend.Client
	manager *runtime.Manager
}

// NewService builds the MM service.
func NewService(st *store.Store, eng *engine.Client, bk *backend.Client, mgr *runtime.Manager) *Service {
	return &Service{store: st, engine: eng, backend: bk, manager: mgr}
}

// walletFor is the synthetic, per-desk engine account. Isolating each desk's
// funds behind its own wallet keeps one desk's inventory and P/L from touching
// another's.
func walletFor(base string, market models.Market) string {
	return fmt.Sprintf("mm:%s:%s", strings.ToUpper(base), strings.ToLower(string(market)))
}

// Create provisions a new desk: the underlying market_maker bot plus the
// market_makers row that funds it. The desk starts disabled and unfunded; the
// admin funds both legs with Deposit(asset="base") / Deposit(asset="quote")
// and starts it with SetEnabled.
func (s *Service) Create(ctx context.Context, base string, market models.Market, symbol string, cfg map[string]string) (*models.MarketMaker, error) {
	base = strings.ToUpper(strings.TrimSpace(base))
	if base == "" {
		return nil, fmt.Errorf("base is required")
	}
	if market != models.Spot && market != models.Futures {
		return nil, fmt.Errorf("market must be SPOT or FUTURES")
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return nil, fmt.Errorf("symbol is required")
	}
	// Refuse desks the engine can never serve: the (symbol, market) pair must
	// have an active order book, else every quote is rejected as unknown symbol.
	tradable, err := s.store.SymbolTradable(ctx, symbol, string(market))
	if err != nil {
		return nil, fmt.Errorf("check symbol: %w", err)
	}
	if !tradable {
		return nil, fmt.Errorf("no active %s order book for %s — pick a listed market", market, symbol)
	}
	// Capture the engine's price/qty granularity so the strategy can snap quotes
	// to it; unsnapped orders are rejected as "not a multiple of tick size".
	tick, lot, err := s.store.SymbolRules(ctx, symbol, string(market))
	if err != nil {
		return nil, fmt.Errorf("load symbol rules: %w", err)
	}
	wallet := walletFor(base, market)
	// The engine risk-locks this desk wallet by its literal id against Dex-
	// Backend's user_balances, whose foreign key requires a matching users row.
	// Provision it up front so the first Deposit's balance credit succeeds.
	if err := s.backend.EnsureUser(ctx, wallet); err != nil {
		return nil, fmt.Errorf("ensure backend user: %w", err)
	}

	// Seed defaults so a fresh desk's strategy config is valid; any admin-supplied
	// keys override. investment is NOT seeded here — a desk's quote budget is its
	// quote_amount, which starts at 0 and only grows via Deposit(asset="quote").
	// The desk therefore can't start until funded, which is the intended invariant.
	merged := strategy.MMDefaults()
	for k, v := range cfg {
		if v != "" {
			merged[k] = v
		}
	}
	merged["symbol"] = symbol
	if tick.IsPositive() {
		merged["_tickSize"] = tick.String()
	}
	if lot.IsPositive() {
		merged["_lotSize"] = lot.String()
	}
	delete(merged, "investment")

	bot := &models.Bot{
		UserID:        "admin",
		WalletAddress: wallet,
		Name:          fmt.Sprintf("MM %s %s", base, market),
		Strategy:      "market_maker",
		Market:        market,
		Symbol:        symbol,
		Investment:    "0",
		Config:        merged,
		IsPublic:      false,
		Status:        models.StatusStopped,
	}
	if err := s.store.Create(ctx, bot); err != nil {
		return nil, fmt.Errorf("create mm bot: %w", err)
	}

	desk := &models.MarketMaker{
		Base:          base,
		Market:        market,
		Symbol:        symbol,
		WalletAddress: wallet,
		BotID:         bot.ID,
		BaseAmount:    "0",
		QuoteAmount:   "0",
		Enabled:       false,
	}
	if err := s.store.CreateMM(ctx, desk); err != nil {
		// Roll back the orphan bot so a failed desk create leaves no trace.
		_ = s.store.Delete(ctx, bot.ID)
		return nil, fmt.Errorf("create mm desk: %w", err)
	}
	return desk, nil
}

// legAsset resolves the logical leg name ("base" or "quote") to the concrete
// asset symbol for this desk (e.g. "BTC" / "USDB").
func legAsset(desk *models.MarketMaker, leg string) (string, error) {
	switch leg {
	case "base":
		return desk.Base, nil
	case "quote":
		return collateralAsset(desk.Market), nil
	default:
		return "", fmt.Errorf("asset must be %q or %q", "base", "quote")
	}
}

// Deposit records an admin-attested capital add to one leg (leg: "base" or
// "quote") of a desk. It credits the MM wallet's real Dex-Backend Postgres
// balance for that specific asset (the authoritative balance the engine risk-
// locks orders against), mirrors the credit into the engine's in-memory
// ledger, bumps that leg's tracked amount with an audit row, and — for the
// quote leg only — syncs the bot's investment budget (the strategy's bid-side
// notional; the base leg has no equivalent single number, it's tracked
// directly as inventory). The backend credit runs first; each later step
// compensates the prior on failure so the invariant holds.
func (s *Service) Deposit(ctx context.Context, deskID, leg string, amount decimal.Decimal, adminID, note string) (*models.MarketMaker, error) {
	desk, err := s.store.GetMM(ctx, deskID)
	if err != nil {
		return nil, err
	}
	if !amount.IsPositive() {
		return nil, fmt.Errorf("amount must be positive")
	}
	asset, err := legAsset(desk, leg)
	if err != nil {
		return nil, err
	}
	// Idempotent: covers desks provisioned before the users row existed.
	if err := s.backend.EnsureUser(ctx, desk.WalletAddress); err != nil {
		return nil, fmt.Errorf("ensure backend user: %w", err)
	}
	if err := s.backend.CreditBalance(ctx, desk.WalletAddress, asset, amount); err != nil {
		return nil, fmt.Errorf("backend credit: %w", err)
	}
	if err := s.engine.LedgerSync(ctx, desk.WalletAddress, asset, amount.String(), "credit"); err != nil {
		_ = s.backend.CreditBalance(ctx, desk.WalletAddress, asset, amount.Neg())
		return nil, fmt.Errorf("engine credit: %w", err)
	}
	next, err := s.store.Fund(ctx, deskID, leg, "deposit", amount, decimal.Zero, adminID, note)
	if err != nil {
		// Credited both ledgers already; compensate so neither drifts above the
		// DB source of truth.
		_ = s.engine.LedgerSync(ctx, desk.WalletAddress, asset, amount.String(), "debit")
		_ = s.backend.CreditBalance(ctx, desk.WalletAddress, asset, amount.Neg())
		return nil, err
	}
	if leg == "quote" {
		_ = s.store.UpdateInvestment(ctx, desk.BotID, next.String())
		desk.QuoteAmount = next.String()
	} else {
		desk.BaseAmount = next.String()
	}
	return desk, nil
}

// Withdraw records an admin-attested capital removal from one leg ("base" or
// "quote") of a desk. It reads that leg's available (unreserved) balance from
// the engine and refuses to pull capital that is locked behind resting
// quotes, then debits the ledger and lowers that leg's tracked amount with an
// audit row.
func (s *Service) Withdraw(ctx context.Context, deskID, leg string, amount decimal.Decimal, adminID, note string) (*models.MarketMaker, error) {
	desk, err := s.store.GetMM(ctx, deskID)
	if err != nil {
		return nil, err
	}
	if !amount.IsPositive() {
		return nil, fmt.Errorf("amount must be positive")
	}
	asset, err := legAsset(desk, leg)
	if err != nil {
		return nil, err
	}
	bal, err := s.engine.Balance(ctx, desk.WalletAddress, asset)
	if err != nil {
		return nil, fmt.Errorf("engine balance: %w", err)
	}
	// floor = allocated - available: the reserved portion the withdraw must not
	// breach. Fund() rejects if (allocated - amount) < floor, i.e. amount >
	// available.
	cur := desk.QuoteAmount
	if leg == "base" {
		cur = desk.BaseAmount
	}
	alloc, _ := decimal.NewFromString(cur)
	floor := alloc.Sub(bal.Available)
	next, err := s.store.Fund(ctx, deskID, leg, "withdraw", amount, floor, adminID, note)
	if err != nil {
		return nil, err
	}
	if err := s.engine.LedgerSync(ctx, desk.WalletAddress, asset, amount.String(), "debit"); err != nil {
		// DB already lowered; restore it so it doesn't sink below the ledger.
		_, _ = s.store.Fund(ctx, deskID, leg, "deposit", amount, decimal.Zero, adminID, "revert: engine debit failed")
		return nil, fmt.Errorf("engine debit: %w", err)
	}
	if err := s.backend.CreditBalance(ctx, desk.WalletAddress, asset, amount.Neg()); err != nil {
		// Engine debited and DB lowered; roll both back so all three agree.
		_ = s.engine.LedgerSync(ctx, desk.WalletAddress, asset, amount.String(), "credit")
		_, _ = s.store.Fund(ctx, deskID, leg, "deposit", amount, decimal.Zero, adminID, "revert: backend debit failed")
		return nil, fmt.Errorf("backend debit: %w", err)
	}
	if leg == "quote" {
		_ = s.store.UpdateInvestment(ctx, desk.BotID, next.String())
		desk.QuoteAmount = next.String()
	} else {
		desk.BaseAmount = next.String()
	}
	return desk, nil
}

// SetEnabled starts or stops a desk's quoting. Enabling starts the underlying
// bot; disabling stops it (cancelling resting quotes).
func (s *Service) SetEnabled(ctx context.Context, deskID string, enabled bool) error {
	desk, err := s.store.GetMM(ctx, deskID)
	if err != nil {
		return err
	}
	if enabled {
		// A desk may be enabled after funding without a process restart.
		// Reconcile the engine ledger against whatever base/quote it was
		// actually funded with first.
		if err := s.Recredit(ctx); err != nil {
			return fmt.Errorf("reconcile desk balances: %w", err)
		}
		if err := s.manager.Start(ctx, desk.BotID); err != nil {
			return err
		}
	} else {
		if err := s.manager.Stop(ctx, desk.BotID); err != nil {
			return err
		}
	}
	return s.store.SetMMEnabled(ctx, deskID, enabled)
}

// Delete tears down a desk: it stops the bot if running, withdraws any residual
// ledger balance so the engine doesn't hold orphaned funds, then removes the
// desk and its underlying bot. Funding-ledger audit rows are retained.
func (s *Service) Delete(ctx context.Context, deskID string) error {
	desk, err := s.store.GetMM(ctx, deskID)
	if err != nil {
		return err
	}
	if s.manager.IsRunning(desk.BotID) {
		if err := s.manager.Stop(ctx, desk.BotID); err != nil {
			return fmt.Errorf("stop desk: %w", err)
		}
	}
	// Reclaim the wallet fully: debit the engine mirror by each leg's funded
	// amount and zero the authoritative Postgres balance (including any locks
	// orphaned by an engine restart), so the wallet id can be safely reused by
	// a future desk without inheriting a ghost balance.
	if amt, aerr := decimal.NewFromString(desk.QuoteAmount); aerr == nil && amt.IsPositive() {
		_ = s.engine.LedgerSync(ctx, desk.WalletAddress, collateralAsset(desk.Market), amt.String(), "debit")
	}
	_ = s.backend.ResetBalance(ctx, desk.WalletAddress, collateralAsset(desk.Market))
	if amt, aerr := decimal.NewFromString(desk.BaseAmount); aerr == nil && amt.IsPositive() {
		_ = s.engine.LedgerSync(ctx, desk.WalletAddress, desk.Base, amt.String(), "debit")
	}
	_ = s.backend.ResetBalance(ctx, desk.WalletAddress, desk.Base)
	if err := s.store.DeleteMM(ctx, deskID); err != nil {
		return err
	}
	return s.store.Delete(ctx, desk.BotID)
}

// SetAllEnabled starts or stops every desk. It applies to each desk in turn and
// returns the first error, having still attempted the rest.
func (s *Service) SetAllEnabled(ctx context.Context, enabled bool) error {
	desks, err := s.store.ListMM(ctx)
	if err != nil {
		return err
	}
	var firstErr error
	for i := range desks {
		if err := s.SetEnabled(ctx, desks[i].ID, enabled); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// UpdateConfig retunes a desk's strategy params (spread, levels, etc.). The new
// config is validated by building the strategy before it is saved. If the desk
// is running, it is restarted so the new params take effect immediately.
func (s *Service) UpdateConfig(ctx context.Context, deskID string, config map[string]string) (*models.MarketMaker, error) {
	desk, err := s.store.GetMM(ctx, deskID)
	if err != nil {
		return nil, err
	}
	bot, err := s.store.Get(ctx, desk.BotID)
	if err != nil {
		return nil, err
	}
	// Merge onto existing config so a partial update doesn't drop keys. symbol is
	// fixed at create time; investment is driven by Deposit/Withdraw (it equals
	// quote_amount), so neither can be retuned here.
	if bot.Config == nil {
		bot.Config = map[string]string{}
	}
	for k, v := range config {
		if k == "symbol" || k == "investment" || strings.HasPrefix(k, "_") || v == "" {
			continue
		}
		bot.Config[k] = v
	}
	if _, err := strategy.Build(bot); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if err := s.store.UpdateConfig(ctx, desk.BotID, bot.Config); err != nil {
		return nil, err
	}
	if s.manager.IsRunning(desk.BotID) {
		if err := s.manager.Stop(ctx, desk.BotID); err != nil {
			return nil, err
		}
		if err := s.manager.Start(ctx, desk.BotID); err != nil {
			return nil, err
		}
	}
	return desk, nil
}

// OpenOrders returns the desk wallet's resting orders from the engine, for the
// admin detail view.
func (s *Service) OpenOrders(ctx context.Context, deskID string) ([]engine.OpenOrder, error) {
	desk, err := s.store.GetMM(ctx, deskID)
	if err != nil {
		return nil, err
	}
	return s.engine.OpenOrders(ctx, desk.WalletAddress)
}

// Recredit clears restart-orphaned locks and re-syncs the bot's investment
// budget after a restart. It does NOT touch either leg's actual balance in
// the engine or the backend.
//
// Two things matter here:
//
//  1. base_amount/quote_amount on the market_makers row are the desk's
//     FUNDING record (what Deposit/Withdraw moved), not a live balance —
//     Dex-Backend's Postgres user_balances is the one authoritative balance
//     (see package doc, and backend.Client's doc comment). Once real fills
//     or locks have touched a desk's wallet, its actual balance can and will
//     differ from base_amount/quote_amount (e.g. a filled sell nets out
//     slightly less base than was funded). Recredit must never push
//     base_amount/quote_amount into the engine ledger as if it were the
//     truth — that would silently overwrite a correct, drifted balance with
//     a stale funding figure and hand the strategy a wrong BaseHeld. The
//     engine's own startup backfill (see matching-engine's Backfill call,
//     which reads directly from the same Postgres row) already primes the
//     in-memory ledger correctly; Recredit's job ends at investment sync.
//
//  2. The matching engine's restart wipes its own live order book, but the
//     durable Postgres `locked` column (what ReplaceLocksFor/LockBalance
//     guard against) is never told the book is gone — nothing calls
//     /internal/balance/release-locks on the engine's behalf. So a desk that
//     had its full inventory locked behind resting quotes before the restart
//     comes back with `locked` still pinned at (or right at) its total
//     balance, leaving zero room for ANY new lock — every requote after a
//     restart then fails "insufficient balance to lock" forever, even though
//     nothing is actually reserved any more (the orders backing that lock no
//     longer exist). ReleaseLocks clears exactly that stale hold, for both
//     legs, before quoting resumes.
func (s *Service) Recredit(ctx context.Context) error {
	desks, err := s.store.ListMM(ctx)
	if err != nil {
		return err
	}
	for i := range desks {
		desk := &desks[i]
		quoteAsset := collateralAsset(desk.Market)
		if err := s.backend.ReleaseLocks(ctx, desk.WalletAddress, quoteAsset); err != nil {
			return fmt.Errorf("release stale quote locks for %s: %w", desk.ID, err)
		}
		if err := s.backend.ReleaseLocks(ctx, desk.WalletAddress, desk.Base); err != nil {
			return fmt.Errorf("release stale base locks for %s: %w", desk.ID, err)
		}
		// investment (the strategy's bid-side quote budget) is the one field
		// legitimately re-derived from quote_amount on every restart — it's
		// strategy config, not a balance, and always meant to track the
		// desk's currently-funded quote leg.
		if quoteAmt, err := decimal.NewFromString(desk.QuoteAmount); err == nil {
			if err := s.store.UpdateInvestment(ctx, desk.BotID, quoteAmt.String()); err != nil {
				return fmt.Errorf("sync bot investment for %s: %w", desk.ID, err)
			}
		}
	}
	return nil
}
