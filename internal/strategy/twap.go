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

// twap slices a single large order into equal-sized market orders spaced evenly
// over a duration to minimise execution impact. Like DCA, market fills are
// recorded at the current mid (approximate), and the bot accumulates a
// position it does not auto-close.
type twap struct {
	state       *State
	symbol      string
	market      models.Market
	side        string
	totalQty    decimal.Decimal
	slices      int
	sliceEvery  time.Duration
	qtyPerSlice decimal.Decimal
	leverage    int
	marginMode  string
}

func twapParams() []models.TemplateParam {
	return []models.TemplateParam{
		{Key: "symbol", Label: "Trading Pair", Type: "text", Required: true, Default: "BI2X-BI2XUSD"},
		{Key: "investment", Label: "Reference Budget (quote)", Type: "number", Required: false, Default: "1000"},
		{Key: "side", Label: "Side", Type: "select", Required: true, Default: "BUY", Options: []string{"BUY", "SELL"}},
		{Key: "totalQty", Label: "Total Quantity (base)", Type: "number", Required: true, Default: "1", Help: "Total base size to execute"},
		{Key: "slices", Label: "Slices", Type: "number", Required: true, Default: "10", Help: "Number of child orders"},
		{Key: "durationSec", Label: "Duration (seconds)", Type: "number", Required: true, Default: "600", Help: "Total time to spread the order over"},
		{Key: "leverage", Label: "Leverage", Type: "number", Required: false, Default: "1"},
		{Key: "marginMode", Label: "Margin Mode", Type: "select", Required: false, Default: "CROSS", Options: []string{"CROSS", "ISOLATED"}},
	}
}

func newTWAP(bot *models.Bot) (Strategy, error) {
	total, err := decimal.NewFromString(cfg(bot, "totalQty"))
	if err != nil || !total.IsPositive() {
		return nil, fmt.Errorf("totalQty must be a positive number")
	}
	slices, err := strconv.Atoi(cfg(bot, "slices"))
	if err != nil || slices < 1 {
		return nil, fmt.Errorf("slices must be a positive integer")
	}
	dur, err := strconv.Atoi(cfg(bot, "durationSec"))
	if err != nil || dur < 1 {
		return nil, fmt.Errorf("durationSec must be a positive integer")
	}
	side := cfg(bot, "side")
	if side != "BUY" && side != "SELL" {
		side = "BUY"
	}
	lev := 1
	if v := cfg(bot, "leverage"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			lev = n
		}
	}
	return &twap{
		state: newStatePtr(), symbol: bot.Symbol, market: bot.Market, side: side,
		totalQty: total, slices: slices, sliceEvery: time.Duration(dur) * time.Second / time.Duration(slices),
		qtyPerSlice: total.Div(decimal.NewFromInt(int64(slices))),
		leverage:    lev, marginMode: cfg(bot, "marginMode"),
	}, nil
}

func (t *twap) Init(ctx context.Context, deps Deps) error {
	t.state.InitDone = true
	t.state.NextSliceMs = time.Now().UnixMilli() // execute the first slice immediately
	t.state.SlicesDone = 0
	return nil
}

func (t *twap) OnTick(ctx context.Context, deps Deps) error {
	if !t.state.InitDone {
		if err := t.Init(ctx, deps); err != nil {
			return err
		}
	}
	// Once tripped, keep failing every tick — not just the once-per-slice
	// ticks that actually attempt an order. The runtime's own halt counter
	// (maxConsecutiveErrors in runtime.go) only accumulates across
	// consecutive ERRORING ticks; between slice attempts, OnTick's normal
	// early-returns below all return nil, which resets that counter back to
	// 0 before the next slice attempt ever fires. Without this check a
	// permanently-failing TWAP never actually halts: `consecutive` in the
	// logs was observed stuck at 1 forever instead of climbing to
	// maxConsecutiveErrors, live, before this fix.
	if t.state.ConsecutiveOrderFails >= maxConsecutiveOrderFailures {
		return fmt.Errorf("twap halted after repeated order failures: %s", t.state.LastOrderError)
	}
	mid := deps.MD.Mid
	if mid.IsZero() {
		return nil
	}
	now := time.Now().UnixMilli()
	if t.state.SlicesDone < t.slices && now >= t.state.NextSliceMs {
		qty := snapToLot(t.qtyPerSlice, deps.Lot)
		if !qty.IsPositive() {
			err := fmt.Errorf("slice qty rounds to zero at lot size %s (investment too small for %d slices)", deps.Lot.String(), t.slices)
			slog.Warn("twap slice qty rounds to zero at lot size, skipping", "symbol", t.symbol, "slice", t.state.SlicesDone)
			t.state.NextSliceMs += t.sliceEvery.Milliseconds()
			if t.state.recordOrderFailure(err) {
				return fmt.Errorf("twap slice repeatedly unsizeable: %w", err)
			}
			return nil
		}
		resp, err := deps.Engine.SubmitOrder(ctx, deps.Account, t.symbol, string(t.market), t.side, "MARKET", decimal.Zero, qty, t.leverage, t.marginMode)
		if err != nil {
			slog.Warn("twap slice failed", "symbol", t.symbol, "slice", t.state.SlicesDone, "error", err)
			if t.state.recordOrderFailure(err) {
				return fmt.Errorf("twap slice repeatedly rejected: %w", err)
			}
		} else {
			t.state.recordOrderSuccess()
			filled := dec(resp.Filled)
			if !filled.IsPositive() {
				filled = qty
			}
			signed := filled
			if t.side == "SELL" {
				signed = filled.Neg()
			}
			t.state.applyBuyFill(signed, mid)
			t.state.recordTrade(time.Now())
			t.state.SlicesDone++
			t.state.NextSliceMs += t.sliceEvery.Milliseconds()
		}
	}
	t.sampleEquity(mid)
	return nil
}

func (t *twap) OnStop(ctx context.Context, deps Deps) error {
	t.state.InitDone = false
	return nil
}

func (t *twap) Snapshot() State { return *t.state }
func (t *twap) Restore(s State) { t.state = &s }

func (t *twap) sampleEquity(mid decimal.Decimal) {
	held := dec(t.state.BaseHeld)
	avg := dec(t.state.AvgEntry)
	unrealized := held.Mul(mid.Sub(avg))
	equity := dec(t.state.RealizedPnL).Add(unrealized)
	t.state.pushEquity(time.Now(), equity)
}
