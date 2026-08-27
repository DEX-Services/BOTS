// Package mm is the market-maker funding and lifecycle service. It sits above
// the store and coordinates the "one number, three places" invariant: a desk's
// allocated_usdc (DB source of truth), the MM wallet's engine-ledger balance
// (in-memory, restart-wiped), and the underlying bot's investment budget must
// always agree.
//
// Funding is admin-attested: the admin moves real USDC into the treasury wallet
// off-platform, then records the number here. Deposits credit the engine ledger
// so the bot can quote against it; withdrawals debit it, guarded against capital
// reserved behind live orders.
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
// Spot markets use USDT; futures use USDC collateral.
func collateralAsset(market models.Market) string {
	if market == models.Spot {
		return "USDT"
	}
	return "USDC"
}

// spotBalances splits a spot desk's capital between the quote currency and
// the base inventory required to maintain a two-sided book.
func spotBalances(capital, indexPrice decimal.Decimal) (quote, base decimal.Decimal) {
	quote = capital.Div(decimal.NewFromInt(2))
	if indexPrice.IsPositive() {
		base = quote.Div(indexPrice)
	}
	return quote, base
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
// admin funds it with Deposit and starts it with SetEnabled.
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
	// keys override. investment is NOT seeded here — a desk's budget is its
	// allocated_usdc, which starts at 0 and only grows via Deposit. The desk
	// therefore can't start until funded, which is the intended invariant.
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
		AllocatedUSDC: "0",
		Enabled:       false,
	}
	if err := s.store.CreateMM(ctx, desk); err != nil {
		// Roll back the orphan bot so a failed desk create leaves no trace.
		_ = s.store.Delete(ctx, bot.ID)
		return nil, fmt.Errorf("create mm desk: %w", err)
	}
	return desk, nil
}

// Deposit records an admin-attested capital add. It credits the MM wallet's
// real Dex-Backend Postgres balance (the authoritative balance the engine risk-
// locks orders against), mirrors the credit into the engine's in-memory ledger,
// bumps allocated_usdc with an audit row, and syncs the bot's investment budget.
// The backend credit runs first; each later step compensates the prior on
// failure so the invariant holds.
func (s *Service) Deposit(ctx context.Context, deskID string, amount decimal.Decimal, adminID, note string) (*models.MarketMaker, error) {
	desk, err := s.store.GetMM(ctx, deskID)
	if err != nil {
		return nil, err
	}
	if !amount.IsPositive() {
		return nil, fmt.Errorf("amount must be positive")
	}
	asset := collateralAsset(desk.Market)
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
	next, err := s.store.Fund(ctx, deskID, "deposit", amount, decimal.Zero, adminID, note)
	if err != nil {
		// Credited both ledgers already; compensate so neither drifts above the
		// DB source of truth.
		_ = s.engine.LedgerSync(ctx, desk.WalletAddress, asset, amount.String(), "debit")
		_ = s.backend.CreditBalance(ctx, desk.WalletAddress, asset, amount.Neg())
		return nil, err
	}
	_ = s.store.UpdateInvestment(ctx, desk.BotID, next.String())
	desk.AllocatedUSDC = next.String()
	return desk, nil
}

// Withdraw records an admin-attested capital removal. It reads the wallet's
// available (unreserved) balance from the engine and refuses to pull capital
// that is locked behind resting quotes, then debits the ledger and lowers
// allocated_usdc with an audit row.
func (s *Service) Withdraw(ctx context.Context, deskID string, amount decimal.Decimal, adminID, note string) (*models.MarketMaker, error) {
	desk, err := s.store.GetMM(ctx, deskID)
	if err != nil {
		return nil, err
	}
	if !amount.IsPositive() {
		return nil, fmt.Errorf("amount must be positive")
	}
	asset := collateralAsset(desk.Market)
	bal, err := s.engine.Balance(ctx, desk.WalletAddress, asset)
	if err != nil {
		return nil, fmt.Errorf("engine balance: %w", err)
	}
	// floor = allocated - available: the reserved portion the withdraw must not
	// breach. Fund() rejects if (allocated - amount) < floor, i.e. amount >
	// available.
	alloc, _ := decimal.NewFromString(desk.AllocatedUSDC)
	floor := alloc.Sub(bal.Available)
	next, err := s.store.Fund(ctx, deskID, "withdraw", amount, floor, adminID, note)
	if err != nil {
		return nil, err
	}
	if err := s.engine.LedgerSync(ctx, desk.WalletAddress, asset, amount.String(), "debit"); err != nil {
		// DB already lowered; restore it so it doesn't sink below the ledger.
		_, _ = s.store.Fund(ctx, deskID, "deposit", amount, decimal.Zero, adminID, "revert: engine debit failed")
		return nil, fmt.Errorf("engine debit: %w", err)
	}
	if err := s.backend.CreditBalance(ctx, desk.WalletAddress, asset, amount.Neg()); err != nil {
		// Engine debited and DB lowered; roll both back so all three agree.
		_ = s.engine.LedgerSync(ctx, desk.WalletAddress, asset, amount.String(), "credit")
		_, _ = s.store.Fund(ctx, deskID, "deposit", amount, decimal.Zero, adminID, "revert: backend debit failed")
		return nil, fmt.Errorf("backend debit: %w", err)
	}
	_ = s.store.UpdateInvestment(ctx, desk.BotID, next.String())
	desk.AllocatedUSDC = next.String()
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
		// A desk may be enabled after funding without a process restart. Reconcile
		// its durable quote/base inventory first so Spot has the 50/50 USDT/BTC
		// split required for a two-sided book.
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
	// Reclaim the wallet fully: debit the engine mirror by its allocated amount
	// and zero the authoritative Postgres balance (including any locks orphaned
	// by an engine restart), so the wallet id can be safely reused by a future
	// desk without inheriting a ghost balance.
	if alloc, aerr := decimal.NewFromString(desk.AllocatedUSDC); aerr == nil && alloc.IsPositive() {
		_ = s.engine.LedgerSync(ctx, desk.WalletAddress, collateralAsset(desk.Market), alloc.String(), "debit")
	}
	_ = s.backend.ResetBalance(ctx, desk.WalletAddress, collateralAsset(desk.Market))
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
	// allocated_usdc), so neither can be retuned here.
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

// Recredit reconciles the durable desk wallets before the matching engine
// starts. The engine then performs one backfill of those balances into its
// in-memory ledger. Keeping those phases separate prevents a restart from
// crediting the engine twice.
//
// It also releases any Postgres-side locks orphaned by the same engine restart.
// The engine wipes its in-memory book on restart and never sends the matching
// /unlock for the orders it forgot, so the durable locked_usdc stays pinned at
// the pre-restart holds and every new quote's balance lock fails with a 409.
// Since the book is empty at this point, zeroing the locks is the truthful
// reconciliation and matches the free=allocated state the mirror is set to.
// This relies on the same "engine just restarted" precondition as the recredit
// above; do NOT restart bots alone against a live engine holding real quotes.
func (s *Service) Recredit(ctx context.Context) error {
	desks, err := s.store.ListMM(ctx)
	if err != nil {
		return err
	}
	for i := range desks {
		alloc, err := decimal.NewFromString(desks[i].AllocatedUSDC)
		if err != nil || !alloc.IsPositive() {
			continue
		}
		// Restore the durable ledger and the engine mirror from the same desk
		// allocation. The backend is what approves order locks, so restoring only
		// the engine ledger would leave a restarted desk able to quote in memory
		// but unable to reserve its real collateral.
		asset := collateralAsset(desks[i].Market)
		quoteCapital := alloc
		if desks[i].Market == models.Spot {
			idx := s.manager.IndexSnapshot(ctx, desks[i].Symbol)
			if !idx.Fresh || !idx.Price.IsPositive() {
				return fmt.Errorf("spot desk %s has no fresh index price for inventory provisioning", desks[i].ID)
			}
			var baseCapital decimal.Decimal
			quoteCapital, baseCapital = spotBalances(alloc, idx.Price)
			if err := s.backend.SyncBalance(ctx, desks[i].WalletAddress, desks[i].Base, baseCapital); err != nil {
				return fmt.Errorf("sync base inventory for %s: %w", desks[i].ID, err)
			}
			// The engine's in-memory ledger is restart-wiped and only refilled once,
			// from Postgres, by its own startup backfill — which races this Recredit
			// (the engine typically boots and backfills before Recredit finishes, so
			// it captures the pre-recredit balance). Left unsynced, the engine keeps
			// quoting the strategy's next Init() off that stale figure while the
			// backend's real balance is the fresh one Recredit just wrote, so the
			// very first requote's lock request mismatches and every subsequent one
			// fails "insufficient BTC balance to lock" forever. Push the same target
			// into the engine ledger so both sides agree again.
			if err := s.syncEngineLedger(ctx, desks[i].WalletAddress, desks[i].Base, baseCapital); err != nil {
				return fmt.Errorf("sync engine ledger for %s: %w", desks[i].ID, err)
			}
			// Recredit deliberately resets the desk to a new 50/50 USDT/BTC
			// inventory baseline. Seed the strategy's cost basis with that BTC at
			// the same index price; otherwise its first sell treats provisioned BTC
			// as free inventory and reports the entire sale value as "profit".
			state := strategy.State{
				OpenOrders:  map[string]strategy.OrderRef{},
				BaseHeld:    baseCapital.String(),
				QuoteCost:   baseCapital.Mul(idx.Price).String(),
				AvgEntry:    idx.Price.String(),
				RealizedPnL: "0",
			}
			if err := s.store.SaveState(ctx, desks[i].BotID, state, models.NewStats()); err != nil {
				return fmt.Errorf("reset spot accounting baseline for %s: %w", desks[i].ID, err)
			}
		}
		// The strategy's investment is the quote budget for bids, not the
		// desk's total capital. For spot desks half of the capital is held as
		// base inventory for asks, so keeping the original total here would make
		// each ladder reserve twice the USDT actually available.
		if err := s.store.UpdateInvestment(ctx, desks[i].BotID, quoteCapital.String()); err != nil {
			return fmt.Errorf("sync bot investment for %s: %w", desks[i].ID, err)
		}
		if err := s.backend.SyncBalance(ctx, desks[i].WalletAddress, asset, quoteCapital); err != nil {
			return fmt.Errorf("sync backend balance for %s: %w", desks[i].ID, err)
		}
		if err := s.syncEngineLedger(ctx, desks[i].WalletAddress, asset, quoteCapital); err != nil {
			return fmt.Errorf("sync engine ledger for %s: %w", desks[i].ID, err)
		}
	}
	return nil
}

// syncEngineLedger brings the engine's in-memory ledger balance for
// (account, asset) to exactly target, mirroring what SyncBalanceFor just set
// in the backend's Postgres row. The engine only exposes credit/debit deltas
// (see /internal/ledger/sync), not an absolute set, so the delta is computed
// against whatever the engine currently holds — which may itself be stale
// after a restart (see the caller's comment).
func (s *Service) syncEngineLedger(ctx context.Context, account, asset string, target decimal.Decimal) error {
	current, err := s.engine.Balance(ctx, account, asset)
	if err != nil {
		return fmt.Errorf("read engine balance: %w", err)
	}
	delta := target.Sub(current.Balance)
	if delta.IsZero() {
		return nil
	}
	if delta.IsPositive() {
		return s.engine.LedgerSync(ctx, account, asset, delta.String(), "credit")
	}
	return s.engine.LedgerSync(ctx, account, asset, delta.Neg().String(), "debit")
}
