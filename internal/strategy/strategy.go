// Package strategy defines the bot strategy interface, the registry of
// create-able strategies, and the shared runtime state + accounting helpers
// every strategy uses.
//
// A Strategy is a single bot instance's brain. The runtime calls OnTick on
// every market-data wake for the bot's symbol; the strategy inspects the
// latest price, detects fills of its own resting orders, and places/cancels
// orders against the matching engine on behalf of the user. All mutable state
// lives in State, which the runtime periodically persists to Postgres so a
// crash or restart resumes the bot in place.
package strategy

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/dex/bots/internal/engine"
	"github.com/dex/bots/internal/marketdata"
	"github.com/dex/bots/internal/models"
	"github.com/shopspring/decimal"
)

// Deps is everything a strategy needs on a tick. The runtime fills MD with the
// latest market-data snapshot for the bot's symbol before each OnTick call.
type Deps struct {
	Engine  *engine.Client
	Account string
	Bot     *models.Bot
	MD      marketdata.Snapshot
}

// Strategy is the interface every bot strategy implements.
type Strategy interface {
	Init(ctx context.Context, deps Deps) error
	OnTick(ctx context.Context, deps Deps) error
	OnStop(ctx context.Context, deps Deps) error
	Snapshot() State
	Restore(s State)
}

// State is the persisted runtime state of a bot. Fields unused by a given
// strategy stay zero/empty. Stored as JSONB in the bots table.
type State struct {
	OpenOrders    map[string]OrderRef `json:"openOrders"`
	BaseHeld      string              `json:"baseHeld"`
	QuoteCost     string              `json:"quoteCost"`
	RealizedPnL   string              `json:"realizedPnl"`
	MatchedTrades int                 `json:"matchedTrades"`
	TradeTimes    []int64             `json:"tradeTimes"`
	Equity        []EquityPoint        `json:"equity"`
	LastBuyMs     int64               `json:"lastBuyMs"`
	SlicesDone    int                 `json:"slicesDone"`
	NextSliceMs   int64               `json:"nextSliceMs"`
	AvgEntry      string              `json:"avgEntry"`
	InitDone      bool                `json:"initDone"`
}

// OrderRef is a tracked resting order placed by the bot.
type OrderRef struct {
	OrderID string `json:"orderId"`
	Side    string `json:"side"`
	Price   string `json:"price"`
	Qty     string `json:"qty"`
	Level   int    `json:"level"`
	Kind    string `json:"kind"`
}

// EquityPoint is one timestamped equity sample for drawdown calculation.
type EquityPoint struct {
	Ms  int64  `json:"ms"`
	Val string `json:"val"`
}

// newState returns a zero-valued State with decimal fields initialised.
func newState() State {
	return State{
		OpenOrders:  map[string]OrderRef{},
		BaseHeld:     "0",
		QuoteCost:    "0",
		RealizedPnL:  "0",
		AvgEntry:     "0",
	}
}

// newStatePtr returns a pointer to a fresh state.
func newStatePtr() *State { s := newState(); return &s }

// dec parses a decimal, returning zero on error (defensive; configs are
// validated up front so this should never fire at runtime).
func dec(s string) decimal.Decimal {
	d, _ := decimal.NewFromString(s)
	return d
}

// registry maps strategy key -> factory. Factories validate the bot's config.
var registry = map[string]func(bot *models.Bot) (Strategy, error){
	"spot_grid":     newGrid,
	"futures_grid":  newGrid,
	"spot_dca":      newDCA,
	"futures_dca":   newDCA,
	"futures_twap":  newTWAP,
}

// availableStrategies is the ordered list of strategy keys, including the
// "coming soon" ones (with available=false) so the UI can render every card.
var availableStrategies = map[string]bool{
	"spot_grid": true, "futures_grid": true, "spot_dca": true,
	"futures_dca": true, "futures_twap": true,
}

// Build constructs a strategy for a bot, validating its configuration.
func Build(bot *models.Bot) (Strategy, error) {
	factory, ok := registry[bot.Strategy]
	if !ok {
		return nil, fmt.Errorf("unknown strategy %q", bot.Strategy)
	}
	if !availableStrategies[bot.Strategy] {
		return nil, fmt.Errorf("strategy %q is not available yet", bot.Strategy)
	}
	return factory(bot)
}

// IsAvailable reports whether a strategy key can be created right now.
func IsAvailable(key string) bool { return availableStrategies[key] }

// Templates returns all strategy templates (available + coming-soon) for the
// create-bot UI, in the order the frontend already renders them.
func Templates() []models.Template {
	return []models.Template{
		{Key: "spot_grid", Title: "Spot Grid", Desc: "Buy low and sell high with 24/7 range trading.", Category: "Spot", Available: true, Params: gridParams(true)},
		{Key: "futures_grid", Title: "Futures Grid", Desc: "Automate long and short futures grids.", Category: "Futures", Available: true, Params: gridParams(false)},
		{Key: "position_snowball", Title: "Position Snowball", Desc: "Compound floating profits into larger positions.", Category: "Futures", Available: false},
		{Key: "futures_dca", Title: "Futures DCA", Desc: "Auto-scale entries and reduce timing risk.", Category: "Futures", Available: true, Params: dcaParams(false)},
		{Key: "arbitrage", Title: "Arbitrage Bot", Desc: "Capture price and funding spread opportunities.", Category: "Futures", Available: false},
		{Key: "rebalancing", Title: "Rebalancing Bot", Desc: "Keep a multi-coin portfolio aligned automatically.", Category: "Spot", Available: false},
		{Key: "spot_dca", Title: "Spot DCA", Desc: "Lower average entry cost with recurring buys.", Category: "Spot", Available: true, Params: dcaParams(true)},
		{Key: "spot_algo", Title: "Spot Algo Orders", Desc: "Split large spot orders into smaller blocks.", Category: "Spot", Available: false},
		{Key: "futures_twap", Title: "Futures TWAP", Desc: "Reduce execution impact with time-sliced orders.", Category: "Futures", Available: true, Params: twapParams()},
		{Key: "futures_vp", Title: "Futures VP", Desc: "Match order size to market urgency levels.", Category: "Futures", Available: false},
	}
}

// ----- shared accounting helpers (avg-cost net-inventory method) -----

// applyBuyFill records a buy fill: base inventory grows, cost basis grows.
func (s *State) applyBuyFill(qty, price decimal.Decimal) {
	held := dec(s.BaseHeld).Add(qty)
	cost := dec(s.QuoteCost).Add(qty.Mul(price))
	s.BaseHeld = held.String()
	s.QuoteCost = cost.String()
	if !held.IsZero() {
		s.AvgEntry = cost.Div(held).String()
	}
}

// applySellFill records a sell fill against avg cost and realizes PnL.
func (s *State) applySellFill(qty, price decimal.Decimal) {
	held := dec(s.BaseHeld)
	avg := decimal.Zero
	if !held.IsZero() {
		avg = dec(s.QuoteCost).Div(held)
	}
	pnl := qty.Mul(price.Sub(avg))
	s.RealizedPnL = dec(s.RealizedPnL).Add(pnl).String()
	newHeld := held.Sub(qty)
	newCost := dec(s.QuoteCost).Sub(qty.Mul(avg))
	if newHeld.IsZero() || newHeld.Sign() != held.Sign() {
		// Flipped through zero: reset cost basis to avoid nonsense avg.
		newCost = decimal.Zero
	}
	if newHeld.IsZero() {
		s.AvgEntry = "0"
	} else {
		s.AvgEntry = newCost.Div(newHeld).String()
	}
	s.BaseHeld = newHeld.String()
	s.QuoteCost = newCost.String()
}

// recordTrade stamps a fill and prunes the 24h trade-time window.
func (s *State) recordTrade(now time.Time) {
	s.MatchedTrades++
	cutoff := now.Add(-24 * time.Hour).UnixMilli()
	kept := s.TradeTimes[:0]
	for _, t := range s.TradeTimes {
		if t >= cutoff {
			kept = append(kept, t)
		}
	}
	s.TradeTimes = append(kept, now.UnixMilli())
}

// Trades24h returns how many fills occurred in the last 24h.
func (s *State) Trades24h(now time.Time) int {
	cutoff := now.Add(-24 * time.Hour).UnixMilli()
	n := 0
	for _, t := range s.TradeTimes {
		if t >= cutoff {
			n++
		}
	}
	return n
}

// pushEquity samples current equity and prunes to the last 7 days.
func (s *State) pushEquity(now time.Time, equity decimal.Decimal) {
	cutoff := now.Add(-7 * 24 * time.Hour).UnixMilli()
	kept := s.Equity[:0]
	for _, p := range s.Equity {
		if p.Ms >= cutoff {
			kept = append(kept, p)
		}
	}
	s.Equity = append(kept, EquityPoint{Ms: now.UnixMilli(), Val: equity.String()})
}

// MaxDrawdown computes the max peak-to-trough drawdown (%) over the equity
// samples, capped to the 7-day window maintained by pushEquity.
func (s *State) MaxDrawdown() decimal.Decimal {
	if len(s.Equity) == 0 {
		return decimal.Zero
	}
	pts := make([]EquityPoint, len(s.Equity))
	copy(pts, s.Equity)
	sort.Slice(pts, func(i, j int) bool { return pts[i].Ms < pts[j].Ms })
	peak := dec(pts[0].Val)
	mdd := decimal.Zero
	for _, p := range pts {
		v := dec(p.Val)
		if v.GreaterThan(peak) {
			peak = v
		}
		if peak.IsPositive() {
			dd := peak.Sub(v).Div(peak).Mul(decimal.NewFromInt(100))
			if dd.GreaterThan(mdd) {
				mdd = dd
			}
		}
	}
	return mdd
}
