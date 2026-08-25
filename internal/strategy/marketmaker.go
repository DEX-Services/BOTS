package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/dex/bots/internal/models"
	"github.com/shopspring/decimal"
)

// defaultMinSpreadBps is the conservative floor a desk's half-spread must clear
// to stay profitable after a round-trip taker fee (~0.1% on each side ≈ 20 bps
// round trip; a 10 bps half-spread = 20 bps captured, breakeven). Overridable
// per desk via the minSpreadBps config key for markets with lower fees.
var defaultMinSpreadBps = decimal.NewFromInt(10)

// marketMaker provides two-sided liquidity by continuously quoting a ladder of
// limit orders around the INDEX price (from Price-Fetcher via deps.Index),
// NOT the engine's own book mid. Quoting off the book would be circular — the
// bot's own resting orders are what set the book mid.
//
// Each tick it: (1) refuses to quote if the index is stale, cancelling any
// resting quotes; (2) re-quotes the ladder when the index has drifted beyond a
// threshold; (3) detects fills of its resting quotes and accounts inventory
// with the shared avg-cost helpers.
type marketMaker struct {
	state        *State
	symbol       string
	base         string
	market       models.Market
	investment   decimal.Decimal
	spreadBps    decimal.Decimal
	levels       int
	levelStepBps decimal.Decimal
	maxInventory decimal.Decimal
	leverage     int
	marginMode   string
	// requoteBps is how far (in bps) the index must move from the last quote
	// mid before we cancel + re-quote. Prevents churning on every micro-tick.
	requoteBps decimal.Decimal
	lastMid    decimal.Decimal
	// tick/lot are the engine's price/qty granularity for this symbol. Quotes are
	// snapped to them before submission — the engine rejects orders that aren't a
	// multiple of tick size / lot size. Seeded into config by the MM service at
	// desk creation (keys _tickSize / _lotSize); zero means "don't round".
	tick decimal.Decimal
	lot  decimal.Decimal
}

func mmParams() []models.TemplateParam {
	return []models.TemplateParam{
		{Key: "symbol", Label: "Trading Pair", Type: "text", Required: true, Default: "BTC-USDC", Help: "e.g. BTC-USDC"},
		{Key: "investment", Label: "Investment (quote)", Type: "number", Required: true, Default: "10000", Help: "Total quote budget backing the quotes"},
		{Key: "spreadBps", Label: "Half-Spread (bps)", Type: "number", Required: true, Default: "10", Help: "Distance of the innermost quote from index, in basis points"},
		{Key: "levels", Label: "Levels Per Side", Type: "number", Required: true, Default: "5", Help: "How many ladder levels on each side"},
		{Key: "levelStepBps", Label: "Level Step (bps)", Type: "number", Required: true, Default: "5", Help: "Extra spread added per outer level"},
		{Key: "maxInventory", Label: "Max Inventory (base)", Type: "number", Required: false, Default: "0", Help: "Stop adding to a side past this base-asset holding (0 = no cap)"},
		{Key: "requoteBps", Label: "Re-quote Threshold (bps)", Type: "number", Required: false, Default: "3", Help: "Index must move this far before quotes are refreshed"},
		{Key: "leverage", Label: "Leverage", Type: "number", Required: false, Default: "1", Help: "Futures leverage (e.g. 5)"},
		{Key: "marginMode", Label: "Margin Mode", Type: "select", Required: false, Default: "CROSS", Options: []string{"CROSS", "ISOLATED"}},
	}
}

// MMDefaults returns the default market-maker config (including investment)
// drawn from the template params, so a freshly created desk is valid and
// startable without the admin filling every field. The "symbol" key is
// excluded — it's set per desk from the trading pair.
func MMDefaults() map[string]string {
	d := map[string]string{}
	for _, p := range mmParams() {
		if p.Key == "symbol" || p.Default == "" {
			continue
		}
		d[p.Key] = p.Default
	}
	return d
}

func newMarketMaker(bot *models.Bot) (Strategy, error) {
	investment, err := decimal.NewFromString(bot.Investment)
	if err != nil || !investment.IsPositive() {
		return nil, fmt.Errorf("investment must be a positive number")
	}
	spreadBps, err := decimal.NewFromString(cfg(bot, "spreadBps"))
	if err != nil || !spreadBps.IsPositive() {
		return nil, fmt.Errorf("spreadBps must be a positive number")
	}
	// Half-spread must clear the round-trip taker fee, else every filled quote
	// loses money. Engine per-symbol fees aren't visible here, so guard against
	// a conservative floor (overridable via config for low-fee markets).
	minSpread := defaultMinSpreadBps
	if v := cfg(bot, "minSpreadBps"); v != "" {
		if d, err := decimal.NewFromString(v); err == nil && d.IsPositive() {
			minSpread = d
		}
	}
	if spreadBps.LessThan(minSpread) {
		return nil, fmt.Errorf("spreadBps (%s) must be at least minSpreadBps (%s) to clear fees", spreadBps, minSpread)
	}
	levels, err := strconv.Atoi(cfg(bot, "levels"))
	if err != nil || levels < 1 {
		return nil, fmt.Errorf("levels must be an integer >= 1")
	}
	stepBps, err := decimal.NewFromString(cfg(bot, "levelStepBps"))
	if err != nil || stepBps.IsNegative() {
		return nil, fmt.Errorf("levelStepBps must be a non-negative number")
	}
	maxInv := decimal.Zero
	if v := cfg(bot, "maxInventory"); v != "" {
		if d, err := decimal.NewFromString(v); err == nil && !d.IsNegative() {
			maxInv = d
		}
	}
	requoteBps := decimal.NewFromInt(3)
	if v := cfg(bot, "requoteBps"); v != "" {
		if d, err := decimal.NewFromString(v); err == nil && d.IsPositive() {
			requoteBps = d
		}
	}
	lev := 1
	if bot.Market == models.Futures {
		if v := cfg(bot, "leverage"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 1 {
				lev = n
			}
		}
	}
	return &marketMaker{
		state: newStatePtr(), symbol: bot.Symbol, base: baseAsset(bot.Symbol),
		market: bot.Market, investment: investment, spreadBps: spreadBps,
		levels: levels, levelStepBps: stepBps, maxInventory: maxInv,
		leverage: lev, marginMode: cfg(bot, "marginMode"), requoteBps: requoteBps,
		tick: decConfig(bot, "_tickSize"), lot: decConfig(bot, "_lotSize"),
	}, nil
}

// decConfig parses a decimal config key, returning zero when absent or invalid.
func decConfig(bot *models.Bot, key string) decimal.Decimal {
	if d, err := decimal.NewFromString(cfg(bot, key)); err == nil {
		return d
	}
	return decimal.Zero
}

func (m *marketMaker) Init(ctx context.Context, deps Deps) error {
	// Nothing to seed up front — the first OnTick with a fresh index quotes the
	// ladder. Init just marks the strategy ready.
	m.state.InitDone = true
	return nil
}

func (m *marketMaker) OnTick(ctx context.Context, deps Deps) error {
	idx := deps.Index
	// 1. Freshness guard: never quote on a stale or absent index price.
	if !idx.Fresh || !idx.Price.IsPositive() {
		return m.cancelAll(ctx, deps)
	}
	mid := idx.Price

	// 2. Detect fills of our resting quotes first, so inventory is current
	//    before we decide the next ladder.
	if err := m.detectFills(ctx, deps); err != nil {
		slog.Warn("mm fill-detect failed", "symbol", m.symbol, "error", err)
	}

	// 3. Re-quote only when the index has drifted beyond the threshold (or we
	//    have no quotes yet).
	if len(m.state.OpenOrders) == 0 || m.drifted(mid) {
		if err := m.requote(ctx, deps, mid); err != nil {
			slog.Warn("mm requote failed", "symbol", m.symbol, "error", err)
		}
	}
	m.sampleEquity(mid)
	return nil
}

func (m *marketMaker) OnStop(ctx context.Context, deps Deps) error {
	return m.cancelAll(ctx, deps)
}

func (m *marketMaker) Snapshot() State { return *m.state }
func (m *marketMaker) Restore(s State) { m.state = &s }

// drifted reports whether the index has moved from the last quote mid by more
// than requoteBps.
func (m *marketMaker) drifted(mid decimal.Decimal) bool {
	if m.lastMid.IsZero() {
		return true
	}
	moveBps := mid.Sub(m.lastMid).Abs().Div(m.lastMid).Mul(decimal.NewFromInt(10000))
	return moveBps.GreaterThanOrEqual(m.requoteBps)
}

// requote cancels all resting quotes and places a fresh ladder around mid.
func (m *marketMaker) requote(ctx context.Context, deps Deps, mid decimal.Decimal) error {
	if err := m.cancelAll(ctx, deps); err != nil {
		return err
	}
	held := dec(m.state.BaseHeld)
	// Per-level quote notional: split the budget evenly across all levels/side.
	perLevel := m.investment.Div(decimal.NewFromInt(int64(m.levels)))
	tenK := decimal.NewFromInt(10000)

	for i := 0; i < m.levels; i++ {
		offBps := m.spreadBps.Add(m.levelStepBps.Mul(decimal.NewFromInt(int64(i))))
		off := offBps.Div(tenK)
		bidPrice := m.snapPrice(mid.Mul(decimal.NewFromInt(1).Sub(off)), false)
		askPrice := m.snapPrice(mid.Mul(decimal.NewFromInt(1).Add(off)), true)

		// BUY side — skip if inventory is already at/over the cap.
		if m.maxInventory.IsZero() || held.LessThan(m.maxInventory) {
			if qty := m.snapQty(qtyFor(perLevel, bidPrice)); qty.IsPositive() {
				m.place(ctx, deps, "BUY", i, bidPrice, qty)
			}
		}
		// SELL side. Spot can only sell what it holds; futures may short.
		canSell := m.market == models.Futures || held.IsPositive()
		if canSell && (m.maxInventory.IsZero() || held.GreaterThan(m.maxInventory.Neg())) {
			if qty := m.snapQty(qtyFor(perLevel, askPrice)); qty.IsPositive() {
				m.place(ctx, deps, "SELL", i, askPrice, qty)
			}
		}
	}
	m.lastMid = mid
	return nil
}

// snapPrice rounds a quote price to the symbol's tick size. Bids round DOWN and
// asks round UP so snapping never narrows the spread below the configured
// half-spread (which would risk crossing the index). A zero tick means the
// engine imposed no granularity, so the price passes through unchanged.
func (m *marketMaker) snapPrice(price decimal.Decimal, isAsk bool) decimal.Decimal {
	if m.tick.IsZero() {
		return price
	}
	steps := price.Div(m.tick)
	if isAsk {
		steps = steps.Ceil()
	} else {
		steps = steps.Floor()
	}
	return steps.Mul(m.tick)
}

// snapQty rounds an order quantity DOWN to the symbol's lot size, so the desk
// never quotes more base than the budget backs. A zero lot passes through.
func (m *marketMaker) snapQty(qty decimal.Decimal) decimal.Decimal {
	if m.lot.IsZero() {
		return qty
	}
	return qty.Div(m.lot).Floor().Mul(m.lot)
}

func (m *marketMaker) place(ctx context.Context, deps Deps, side string, level int, price, qty decimal.Decimal) {
	resp, err := deps.Engine.SubmitOrder(ctx, deps.Account, m.symbol, string(m.market), side, "LIMIT", price, qty, m.leverage, m.marginMode)
	if err != nil {
		slog.Warn("mm place failed", "symbol", m.symbol, "side", side, "level", level, "error", err)
		return
	}
	m.state.OpenOrders[resp.OrderID] = OrderRef{
		OrderID: resp.OrderID, Side: side, Price: price.String(), Qty: qty.String(), Level: level, Kind: "mm",
	}
}

// cancelAll cancels every tracked resting quote and clears the map. The cancel
// response carries the order's final filled quantity, so any partial fill that
// accrued before the cancel is accounted here rather than lost. If a cancel
// fails (e.g. the order just fully filled and is no longer cancellable, or the
// engine is mid-shutdown), the order is left tracked so the next tick's
// detectFills reconciles it against authoritative state instead of dropping a
// possible fill.
func (m *marketMaker) cancelAll(ctx context.Context, deps Deps) error {
	for id, ref := range m.state.OpenOrders {
		resp, err := deps.Engine.CancelOrder(ctx, m.symbol, string(m.market), id)
		if err != nil {
			slog.Warn("mm cancel failed", "order", id, "error", err)
			continue
		}
		r := ref
		if m.state.applyFillDelta(&r, dec(resp.Filled), dec(r.Price)).IsPositive() {
			m.state.recordTrade(deps.MD.UpdatedAt)
		}
		delete(m.state.OpenOrders, id)
	}
	return nil
}

// detectFills reconciles tracked quotes against the engine's authoritative
// order state and accounts only the real filled delta. A resting order that has
// partially filled is picked up from the /orders "filled" field; an order that
// has left the book is resolved via /order/status, which distinguishes a true
// (possibly partial) fill from a self-trade-prevention cancel or an order lost
// to an engine restart — both of which fill nothing. This replaces the old
// "vanished ⇒ fully filled" assumption that fabricated PnL whenever the desk
// requoted, self-crossed, or the engine restarted.
func (m *marketMaker) detectFills(ctx context.Context, deps Deps) error {
	open, err := deps.Engine.OpenOrders(ctx, deps.Account)
	if err != nil {
		return err
	}
	resting := map[string]string{} // id -> cumulative filled qty
	for _, o := range open {
		if o.Symbol == m.symbol && o.Market == string(m.market) {
			resting[o.ID] = o.Filled
		}
	}
	for id, ref := range m.state.OpenOrders {
		price := dec(ref.Price)
		if filledStr, ok := resting[id]; ok {
			// Still on the book: account any partial fill that has accrued while
			// it rests, but keep tracking it.
			r := ref
			if m.state.applyFillDelta(&r, dec(filledStr), price).IsPositive() {
				m.state.recordTrade(deps.MD.UpdatedAt)
			}
			m.state.OpenOrders[id] = r
			continue
		}
		// Left the book: resolve its true terminal state before accounting.
		st, err := deps.Engine.OrderStatusByID(ctx, m.symbol, string(m.market), id)
		if err != nil {
			// Transient lookup failure — leave the order tracked and retry next
			// tick rather than guess.
			slog.Warn("mm order-status lookup failed", "symbol", m.symbol, "order", id, "error", err)
			continue
		}
		if !st.Found {
			// Not yet in the durable record (async writer lag). Keep tracking;
			// it will resolve on a later tick.
			continue
		}
		r := ref
		if m.state.applyFillDelta(&r, dec(st.Filled), price).IsPositive() {
			m.state.recordTrade(deps.MD.UpdatedAt)
		}
		delete(m.state.OpenOrders, id)
	}
	return nil
}

// sampleEquity records equity (realized + unrealized vs the index mid) for the
// drawdown/ROI curve.
func (m *marketMaker) sampleEquity(mid decimal.Decimal) {
	held := dec(m.state.BaseHeld)
	avg := dec(m.state.AvgEntry)
	unrealized := held.Mul(mid.Sub(avg))
	equity := dec(m.state.RealizedPnL).Add(unrealized)
	m.state.pushEquity(time.Now(), equity)
}

// qtyFor returns the base qty for a fixed quote notional at price.
func qtyFor(quoteNotional, price decimal.Decimal) decimal.Decimal {
	if price.IsZero() {
		return decimal.Zero
	}
	return quoteNotional.Div(price)
}

// baseAsset extracts the base symbol from a pair like "BTC-USDC" -> "BTC".
// Falls back to the whole string if no separator is present.
func baseAsset(symbol string) string {
	s := strings.ToUpper(strings.TrimSpace(symbol))
	for _, sep := range []string{"-", "/", "_"} {
		if i := strings.Index(s, sep); i > 0 {
			return s[:i]
		}
	}
	return s
}
