package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/dex/bots/internal/models"
	"github.com/shopspring/decimal"
)

// grid runs a price-time grid between a lower and upper bound. Spot grids start
// fully in quote and place buys below mid, flipping each fill into a sell one
// level up (self-funding). Futures grids place both buys below and sells above
// mid (leverage allows shorting) and flip fills the same way. Maker limit
// orders fill at their own limit price, so fill accounting is exact.
type grid struct {
	state       *State
	symbol      string
	market      models.Market
	investment  decimal.Decimal
	lower       decimal.Decimal
	upper       decimal.Decimal
	grids       int
	leverage    int
	marginMode  string
	levels      []decimal.Decimal
	lastMid     decimal.Decimal
	qtyPerLevel func(decimal.Decimal) decimal.Decimal
}

func gridParams(spot bool) []models.TemplateParam {
	params := []models.TemplateParam{
		{Key: "symbol", Label: "Trading Pair", Type: "text", Required: true, Default: "BTC-USDT", Help: "e.g. BTC-USDT"},
		{Key: "investment", Label: "Investment (quote)", Type: "number", Required: true, Default: "1000", Help: "Total quote budget for the grid"},
		{Key: "lowerPrice", Label: "Lower Price", Type: "number", Required: true, Default: "90", Help: "Bottom of the grid range"},
		{Key: "upperPrice", Label: "Upper Price", Type: "number", Required: true, Default: "110", Help: "Top of the grid range"},
		{Key: "grids", Label: "Number of Grids", Type: "number", Required: true, Default: "10", Help: "How many grid levels across the range"},
	}
	if !spot {
		params = append(params,
			models.TemplateParam{Key: "leverage", Label: "Leverage", Type: "number", Required: false, Default: "1", Help: "Futures leverage (e.g. 5)"},
			models.TemplateParam{Key: "marginMode", Label: "Margin Mode", Type: "select", Required: false, Default: "CROSS", Options: []string{"CROSS", "ISOLATED"}},
		)
	}
	return params
}

func newGrid(bot *models.Bot) (Strategy, error) {
	lower, err := decimal.NewFromString(cfg(bot, "lowerPrice"))
	if err != nil || !lower.IsPositive() {
		return nil, fmt.Errorf("lowerPrice must be a positive number")
	}
	upper, err := decimal.NewFromString(cfg(bot, "upperPrice"))
	if err != nil || !upper.IsPositive() || !upper.GreaterThan(lower) {
		return nil, fmt.Errorf("upperPrice must be > lowerPrice")
	}
	grids, err := strconv.Atoi(cfg(bot, "grids"))
	if err != nil || grids < 2 {
		return nil, fmt.Errorf("grids must be an integer >= 2")
	}
	investment, err := decimal.NewFromString(bot.Investment)
	if err != nil || !investment.IsPositive() {
		return nil, fmt.Errorf("investment must be a positive number")
	}
	lev := 1
	if bot.Market == models.Futures {
		if v := cfg(bot, "leverage"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 1 {
				lev = n
			}
		}
	}
	levels := make([]decimal.Decimal, grids+1)
	step := upper.Sub(lower).Div(decimal.NewFromInt(int64(grids)))
	for i := 0; i <= grids; i++ {
		levels[i] = lower.Add(step.Mul(decimal.NewFromInt(int64(i))))
	}
	g := &grid{
		state: newStatePtr(), symbol: bot.Symbol, market: bot.Market,
		investment: investment, lower: lower, upper: upper, grids: grids,
		leverage: lev, marginMode: cfg(bot, "marginMode"), levels: levels,
	}
	// Each grid level risks investment/grids worth of quote. qty at a level
	// is (investment/grids) / price so the quote notional per level is fixed.
	perLevel := investment.Div(decimal.NewFromInt(int64(grids)))
	g.qtyPerLevel = func(price decimal.Decimal) decimal.Decimal {
		if price.IsZero() {
			return decimal.Zero
		}
		return perLevel.Div(price)
	}
	return g, nil
}

func (g *grid) Init(ctx context.Context, deps Deps) error {
	// Resuming an already-seeded grid must not re-seed it. The runtime calls
	// Init once per Start, INCLUDING the Start that resumes a bot after a
	// restart — and by then Restore has already loaded persisted state whose
	// OpenOrders are real orders still resting on the engine. Placing the
	// whole ladder again would double every level: a second set of live
	// orders, a second set of balance locks, and a tracked-order map the
	// strategy would then try to flip twice per price level.
	//
	// OnTick already gates its own deferred seeding on this exact flag; Init
	// was the one path that ignored it. Re-seeding only happens after OnStop,
	// which clears InitDone precisely so a later start re-seeds around the
	// then-current price.
	if g.state.InitDone {
		g.lastMid = deps.MD.Mid
		return nil
	}
	mid := deps.MD.Mid
	if mid.IsZero() {
		// No live two-sided quote (book momentarily empty on one or both
		// sides), but the symbol may still have real trade history — use the
		// last trade price as the seed for the grid's levels rather than
		// refusing to start. A cold symbol that has genuinely never traded
		// still has Last zero too, so this still fails correctly for that
		// case.
		mid = deps.MD.Last
	}
	if mid.IsZero() {
		return fmt.Errorf("no market data for %s; cannot initialise grid", g.symbol)
	}
	var attempted, failed int
	var lastErr error
	for i, p := range g.levels {
		buy := p.LessThan(mid)
		sell := p.GreaterThan(mid)
		// Spot only places buys initially (self-funding); futures places both
		// sides because leverage lets it short without holding base.
		if buy {
			attempted++
			if err := g.place(ctx, deps, "BUY", i, p); err != nil {
				failed++
				lastErr = err
				slog.Warn("grid init buy failed", "symbol", g.symbol, "level", i, "error", err)
			}
		} else if sell && g.market == models.Futures {
			attempted++
			if err := g.place(ctx, deps, "SELL", i, p); err != nil {
				failed++
				lastErr = err
				slog.Warn("grid init sell failed", "symbol", g.symbol, "level", i, "error", err)
			}
		}
	}
	// Every level rejected is a deterministic config problem (undersized
	// investment for the grid count, wrong lot size), not a transient one —
	// surface it now rather than leaving the bot at status=running with an
	// empty, permanently non-trading ladder (confirmed live: a 10-grid bot
	// on this symbol failed all 10 initial orders instantly with no visible
	// error). A partial failure (some levels placed) is left as a warning
	// only, since the grid can still trade on the levels that did place.
	if attempted > 0 && failed == attempted {
		return fmt.Errorf("every initial grid order rejected (last: %w)", lastErr)
	}
	g.state.InitDone = true
	g.lastMid = mid
	return nil
}

func (g *grid) OnTick(ctx context.Context, deps Deps) error {
	if !g.state.InitDone {
		return g.Init(ctx, deps)
	}
	mid := deps.MD.Mid
	if mid.IsZero() {
		return nil
	}
	// Only poll the engine for fills when price has moved since the last check
	// — this keeps /orders traffic O(price-changes) rather than O(bots*ticks).
	if mid.Equal(g.lastMid) {
		return nil
	}
	g.lastMid = mid
	if err := g.detectFillsAndFlip(ctx, deps); err != nil {
		slog.Warn("grid fill-detect failed", "symbol", g.symbol, "error", err)
	}
	g.sampleEquity(mid)
	return nil
}

func (g *grid) OnStop(ctx context.Context, deps Deps) error {
	for id := range g.state.OpenOrders {
		if _, err := deps.Engine.CancelOrder(ctx, deps.Account, g.symbol, string(g.market), id); err != nil {
			slog.Warn("grid stop cancel failed", "order", id, "error", err)
		}
	}
	g.state.OpenOrders = map[string]OrderRef{}
	g.state.InitDone = false // a later start re-seeds the grid around the new price
	return nil
}

func (g *grid) Snapshot() State { return *g.state }
func (g *grid) Restore(s State) { g.state = &s }

func (g *grid) place(ctx context.Context, deps Deps, side string, level int, price decimal.Decimal) error {
	qty := snapToLot(g.qtyPerLevel(price), deps.Lot)
	if !qty.IsPositive() {
		return fmt.Errorf("zero qty at level %d after rounding to lot size", level)
	}
	resp, err := deps.Engine.SubmitOrder(ctx, deps.Account, g.symbol, string(g.market), side, "LIMIT", price, qty, g.leverage, g.marginMode)
	if err != nil {
		return err
	}
	g.state.OpenOrders[resp.OrderID] = OrderRef{
		OrderID: resp.OrderID, Side: side, Price: price.String(), Qty: qty.String(), Level: level, Kind: "grid",
	}
	return nil
}

// detectFillsAndFlip reconciles tracked orders against authoritative engine
// state, accounts only the real filled delta, and places the opposite grid
// order when a level's order actually fills. Partial fills that accrue while an
// order still rests are accounted into inventory but do not flip until the
// order leaves the book fully filled. Orders that leave the book without a fill
// — self-trade-prevention cancels, or orders lost to an engine restart — are
// resolved via /order/status and account nothing, so no phantom flips or PnL.
func (g *grid) detectFillsAndFlip(ctx context.Context, deps Deps) error {
	open, err := deps.Engine.OpenOrders(ctx, deps.Account)
	if err != nil {
		return err
	}
	resting := map[string]string{} // id -> cumulative filled qty
	for _, o := range open {
		if o.Symbol == g.symbol && o.Market == string(g.market) {
			resting[o.ID] = o.Filled
		}
	}
	for id, ref := range g.state.OpenOrders {
		price := dec(ref.Price)
		if filledStr, ok := resting[id]; ok {
			// Still resting: account any accrued partial fill, keep the order.
			r := ref
			if g.state.applyFillDelta(&r, dec(filledStr), price).IsPositive() {
				g.state.recordTrade(deps.MD.UpdatedAt)
			}
			g.state.OpenOrders[id] = r
			continue
		}
		// Left the book: resolve its true terminal state.
		st, err := deps.Engine.OrderStatusByID(ctx, g.symbol, string(g.market), id)
		if err != nil {
			slog.Warn("grid order-status lookup failed", "symbol", g.symbol, "order", id, "error", err)
			continue
		}
		if !st.Found {
			continue // async-writer lag; reconcile on a later tick
		}
		// Found is NOT the same as settled. /order/status falls back to the
		// engine's durable Postgres record once an order leaves the live book,
		// and that row is written by an ASYNC event-log writer: it exists from
		// the moment the order was created (status=OPEN, filled=0) and is only
		// updated to its terminal state some time after the match actually
		// happens. So an order that just filled reports Found=true with a
		// still-OPEN, still-zero-filled row for a window after it vanished
		// from /orders.
		//
		// Treating that as authoritative is destructive and unrecoverable:
		// the code below deletes the order from OpenOrders, so the fill is
		// never accounted (no inventory, no PnL, no matchedTrades) and the
		// level never flips into its opposite order — the grid silently goes
		// one level dark per fill. Confirmed live: a spot_grid BUY at 8.27
		// filled 7.55743 and settled in the engine ledger, while the bot's
		// own state stayed at matchedTrades=0 / baseHeld=0 forever and no
		// SELL was placed.
		//
		// Only a genuinely terminal record is safe to act on. Anything else
		// is lag, and is handled exactly like !Found above: keep the order
		// tracked and re-resolve it on a later tick.
		if !isTerminalOrderStatus(st.Status) {
			continue
		}
		r := ref
		if g.state.applyFillDelta(&r, dec(st.Filled), price).IsPositive() {
			g.state.recordTrade(deps.MD.UpdatedAt)
		}
		delete(g.state.OpenOrders, id)
		// Flip only if the order actually took fills over its lifetime. A
		// zero-filled exit (STP cancel or restart-orphaned) places nothing.
		if !dec(st.Filled).IsPositive() {
			continue
		}
		// The level's order filled: place the opposite order one level toward mid.
		if ref.Side == "BUY" {
			if ref.Level+1 <= g.grids {
				_ = g.place(ctx, deps, "SELL", ref.Level+1, g.levels[ref.Level+1])
			}
		} else {
			if ref.Level-1 >= 0 {
				_ = g.place(ctx, deps, "BUY", ref.Level-1, g.levels[ref.Level-1])
			}
		}
	}
	return nil
}

func (g *grid) sampleEquity(mid decimal.Decimal) {
	held := dec(g.state.BaseHeld)
	avg := dec(g.state.AvgEntry)
	unrealized := held.Mul(mid.Sub(avg))
	equity := dec(g.state.RealizedPnL).Add(unrealized)
	g.state.pushEquity(time.Now(), equity)
}

// cfg reads a config key from the bot with a fallback to empty string.
func cfg(bot *models.Bot, key string) string {
	if v, ok := bot.Config[key]; ok {
		return v
	}
	return ""
}
