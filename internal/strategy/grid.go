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
	mid := deps.MD.Mid
	if mid.IsZero() {
		return fmt.Errorf("no market data for %s; cannot initialise grid", g.symbol)
	}
	for i, p := range g.levels {
		buy := p.LessThan(mid)
		sell := p.GreaterThan(mid)
		// Spot only places buys initially (self-funding); futures places both
		// sides because leverage lets it short without holding base.
		if buy {
			if err := g.place(ctx, deps, "BUY", i, p); err != nil {
				slog.Warn("grid init buy failed", "symbol", g.symbol, "level", i, "error", err)
			}
		} else if sell && g.market == models.Futures {
			if err := g.place(ctx, deps, "SELL", i, p); err != nil {
				slog.Warn("grid init sell failed", "symbol", g.symbol, "level", i, "error", err)
			}
		}
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
		if _, err := deps.Engine.CancelOrder(ctx, g.symbol, string(g.market), id); err != nil {
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
	qty := g.qtyPerLevel(price)
	if !qty.IsPositive() {
		return fmt.Errorf("zero qty at level %d", level)
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

// detectFillsAndFlip fetches the account's open orders, treats any of our
// tracked orders that have vanished as filled (maker fills at limit price),
// accounts the fill, and places the opposite order one level toward mid.
func (g *grid) detectFillsAndFlip(ctx context.Context, deps Deps) error {
	open, err := deps.Engine.OpenOrders(ctx, deps.Account)
	if err != nil {
		return err
	}
	stillOpen := map[string]bool{}
	for _, o := range open {
		if o.Symbol == g.symbol && o.Market == string(g.market) {
			stillOpen[o.ID] = true
		}
	}
	for id, ref := range g.state.OpenOrders {
		if stillOpen[id] {
			continue
		}
		// Filled (or cancelled). We only cancel on stop, so absence while
		// running means a fill at the limit price ref.Price.
		qty := dec(ref.Qty)
		price := dec(ref.Price)
		if ref.Side == "BUY" {
			g.state.applyBuyFill(qty, price)
			g.state.recordTrade(deps.MD.UpdatedAt)
			if ref.Level+1 <= g.grids {
				_ = g.place(ctx, deps, "SELL", ref.Level+1, g.levels[ref.Level+1])
			}
		} else {
			g.state.applySellFill(qty, price)
			g.state.recordTrade(deps.MD.UpdatedAt)
			if ref.Level-1 >= 0 {
				_ = g.place(ctx, deps, "BUY", ref.Level-1, g.levels[ref.Level-1])
			}
		}
		delete(g.state.OpenOrders, id)
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
