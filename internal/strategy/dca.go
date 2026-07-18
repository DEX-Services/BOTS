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

// dca places a fixed-quote market buy (or sell for futures SHORT) on a fixed
// interval, accumulating a position and averaging the entry cost. Market fills
// are recorded at the current mid (the engine /order response does not return
// a fill price), so DCA stats are approximate.
type dca struct {
	state      *State
	symbol     string
	market     models.Market
	amount     decimal.Decimal
	interval   time.Duration
	side       string // BUY (LONG) or SELL (SHORT) — futures only
	leverage   int
	marginMode string
}

func dcaParams(spot bool) []models.TemplateParam {
	params := []models.TemplateParam{
		{Key: "symbol", Label: "Trading Pair", Type: "text", Required: true, Default: "BTC-USDT"},
		{Key: "investment", Label: "Total Budget (quote)", Type: "number", Required: true, Default: "1000", Help: "Reference total budget"},
		{Key: "amount", Label: "Amount per Buy (quote)", Type: "number", Required: true, Default: "100", Help: "Quote spent on each recurring buy"},
		{Key: "intervalSec", Label: "Interval (seconds)", Type: "number", Required: true, Default: "3600", Help: "Time between buys"},
	}
	if !spot {
		params = append(params,
			models.TemplateParam{Key: "side", Label: "Direction", Type: "select", Required: true, Default: "BUY", Options: []string{"BUY", "SELL"}, Help: "BUY = accumulate long, SELL = accumulate short"},
			models.TemplateParam{Key: "leverage", Label: "Leverage", Type: "number", Required: false, Default: "1"},
			models.TemplateParam{Key: "marginMode", Label: "Margin Mode", Type: "select", Required: false, Default: "CROSS", Options: []string{"CROSS", "ISOLATED"}},
		)
	}
	return params
}

func newDCA(bot *models.Bot) (Strategy, error) {
	amount, err := decimal.NewFromString(cfg(bot, "amount"))
	if err != nil || !amount.IsPositive() {
		return nil, fmt.Errorf("amount must be a positive number")
	}
	sec, err := strconv.Atoi(cfg(bot, "intervalSec"))
	if err != nil || sec < 1 {
		return nil, fmt.Errorf("intervalSec must be a positive integer (seconds)")
	}
	side := "BUY"
	lev := 1
	margin := ""
	if bot.Market == models.Futures {
		side = cfg(bot, "side")
		if side == "" {
			side = "BUY"
		}
		if side != "BUY" && side != "SELL" {
			return nil, fmt.Errorf("side must be BUY or SELL")
		}
		if v := cfg(bot, "leverage"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 1 {
				lev = n
			}
		}
		margin = cfg(bot, "marginMode")
	}
	return &dca{
		state: newStatePtr(), symbol: bot.Symbol, market: bot.Market,
		amount: amount, interval: time.Duration(sec) * time.Second,
		side: side, leverage: lev, marginMode: margin,
	}, nil
}

func (d *dca) Init(ctx context.Context, deps Deps) error {
	d.state.InitDone = true
	d.state.LastBuyMs = 0 // buy immediately on the first tick
	return nil
}

func (d *dca) OnTick(ctx context.Context, deps Deps) error {
	if !d.state.InitDone {
		if err := d.Init(ctx, deps); err != nil {
			return err
		}
	}
	mid := deps.MD.Mid
	if mid.IsZero() {
		return nil
	}
	now := time.Now().UnixMilli()
	if d.state.LastBuyMs != 0 && now-d.state.LastBuyMs < d.interval.Milliseconds() {
		d.sampleEquity(mid)
		return nil
	}
	qty := d.amount.Div(mid)
	if !qty.IsPositive() {
		return nil
	}
	resp, err := deps.Engine.SubmitOrder(ctx, deps.Account, d.symbol, string(d.market), d.side, "MARKET", decimal.Zero, qty, d.leverage, d.marginMode)
	if err != nil {
		slog.Warn("dca order failed", "symbol", d.symbol, "error", err)
		d.sampleEquity(mid)
		return nil
	}
	filled := dec(resp.Filled)
	if !filled.IsPositive() {
		filled = qty // engine may omit filled for immediate market fills
	}
	// Market fills record at mid (engine gives no fill price). SELL (short
	// accumulation) is a negative-base buy for accounting purposes.
	signed := filled
	if d.side == "SELL" {
		signed = filled.Neg()
	}
	d.state.applyBuyFill(signed, mid)
	d.state.recordTrade(time.Now())
	d.state.LastBuyMs = now
	d.sampleEquity(mid)
	return nil
}

func (d *dca) OnStop(ctx context.Context, deps Deps) error {
	// DCA leaves no resting orders; nothing to cancel.
	d.state.InitDone = false
	return nil
}

func (d *dca) Snapshot() State { return *d.state }
func (d *dca) Restore(s State) { d.state = &s }

func (d *dca) sampleEquity(mid decimal.Decimal) {
	held := dec(d.state.BaseHeld)
	avg := dec(d.state.AvgEntry)
	unrealized := held.Mul(mid.Sub(avg))
	equity := dec(d.state.RealizedPnL).Add(unrealized)
	d.state.pushEquity(time.Now(), equity)
}
