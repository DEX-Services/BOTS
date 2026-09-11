package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/dex/bots/internal/engine"
	"github.com/dex/bots/internal/models"
	"github.com/shopspring/decimal"
)

// optionsMarketMaker quotes two-sided liquidity across an underlying's whole
// listed option chain (every strike × expiry × CALL/PUT combination the
// engine's GET /option-chain returns), anchored to that endpoint's own
// theoretical Black-Scholes fair value ± a configured spread — never the
// bot's own resting orders, which would be circular the same way marketMaker
// avoids quoting off the engine's own book mid for spot/futures.
//
// This is a genuinely different shape from marketMaker: that strategy quotes
// ONE (symbol, market) order book via the all-or-nothing
// ReplaceMarketMakerLadder endpoint (exactly 10 orders, one reference price).
// An option chain is dozens of INDEPENDENT order books (one per contract),
// each needing its own fair value and its own bid/ask — there is no single
// "the ladder" to replace atomically, so this strategy places/cancels each
// contract's pair of quotes individually via the same /order and /cancel
// endpoints a regular user hits, and reconciles fills via one /orders poll
// per tick (OpenOrders returns every resting order for the account across
// all symbols, which is exactly what a many-order-book desk needs).
type optionsMarketMaker struct {
	state          *State
	underlying     string // e.g. "BTC-BIUSDB", passed as bot.Symbol
	quoteAsset     string
	investment     decimal.Decimal
	spreadBps      decimal.Decimal
	skewPerDelta   decimal.Decimal // price skew (bps of fair value) per unit of net inventory delta
	qtyPerContract decimal.Decimal
	maxContracts   int // cap on distinct contracts quoted at once (0 = no cap beyond the chain's own size)
}

func optionsMMParams() []models.TemplateParam {
	return []models.TemplateParam{
		{Key: "symbol", Label: "Underlying", Type: "text", Required: true, Default: "BTC-BIUSDB", Help: "The underlying spot pair, e.g. BTC-BIUSDB"},
		{Key: "investment", Label: "Investment (quote)", Type: "number", Required: true, Default: "50000", Help: "Total quote budget backing all quoted contracts"},
		{Key: "spreadBps", Label: "Half-Spread (bps of fair value)", Type: "number", Required: true, Default: "300", Help: "Distance of each quote from theoretical fair value, in basis points of premium"},
		{Key: "qtyPerContract", Label: "Contracts per quote", Type: "number", Required: true, Default: "0.1", Help: "Size quoted on each side of every strike/expiry"},
		{Key: "skewPerDeltaBps", Label: "Inventory skew (bps per net delta)", Type: "number", Required: false, Default: "0", Help: "Shifts quotes to lean against net directional exposure (0 = no skew)"},
		{Key: "maxContracts", Label: "Max contracts quoted", Type: "number", Required: false, Default: "0", Help: "Cap on distinct strike/expiry/type combinations quoted at once (0 = quote the whole chain)"},
	}
}

// OptionsMMDefaults mirrors MMDefaults for the options desk's config.
func OptionsMMDefaults() map[string]string {
	d := map[string]string{}
	for _, p := range optionsMMParams() {
		if p.Key == "symbol" || p.Default == "" {
			continue
		}
		d[p.Key] = p.Default
	}
	return d
}

func newOptionsMarketMaker(bot *models.Bot) (Strategy, error) {
	// investment is bot.Investment (the top-level struct field/bots.investment
	// column), NOT cfg(bot, "investment") — matching marketMaker/newGrid's
	// identical read. An MM desk's investment is written via
	// store.UpdateInvestment (mm.Service.recreditDesk, on every enable/
	// restart, kept in step with the desk's funded quote_amount), which
	// targets that column directly; desk creation also deletes "investment"
	// from the strategy config map entirely (see mm.Service.Create) since a
	// desk's budget is never meant to be static config. Reading it from
	// cfg() here instead was a bug — an options desk enabled with the SAME
	// deposit-then-enable flow as any other desk would still see this as
	// "investment must be a positive number" and refuse to start, since
	// cfg(bot, "investment") is always empty right after Create.
	investment, err := decimal.NewFromString(bot.Investment)
	if err != nil || !investment.IsPositive() {
		return nil, fmt.Errorf("investment must be a positive number")
	}
	spreadBps, err := decimal.NewFromString(cfg(bot, "spreadBps"))
	if err != nil || !spreadBps.IsPositive() {
		return nil, fmt.Errorf("spreadBps must be a positive number")
	}
	// Options premiums are much smaller than the underlying's own notional
	// and quoted with a flat-vol theoretical model (see /option-chain), so
	// the round-trip-fee floor marketMaker uses (10 bps of underlying
	// notional) doesn't transfer directly — the floor here is expressed as
	// bps of the PREMIUM itself, and a much wider minimum is needed since a
	// mispriced theoretical value is a bigger risk than a thin taker fee.
	const minSpreadBps = 100 // 1% of premium, minimum
	if spreadBps.LessThan(decimal.NewFromInt(minSpreadBps)) {
		return nil, fmt.Errorf("spreadBps (%s) must be at least %d for options (premiums are thin; a tighter spread risks quoting through the model's own error)", spreadBps, minSpreadBps)
	}
	qtyPerContract, err := decimal.NewFromString(cfg(bot, "qtyPerContract"))
	if err != nil || !qtyPerContract.IsPositive() {
		return nil, fmt.Errorf("qtyPerContract must be a positive number")
	}
	skew := decimal.Zero
	if v := cfg(bot, "skewPerDeltaBps"); v != "" {
		if d, err := decimal.NewFromString(v); err == nil && !d.IsNegative() {
			skew = d
		}
	}
	maxContracts := 0
	if v := cfg(bot, "maxContracts"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxContracts = n
		}
	}
	return &optionsMarketMaker{
		state: newStatePtr(), underlying: bot.Symbol, quoteAsset: "BIUSDB",
		investment: investment, spreadBps: spreadBps, skewPerDelta: skew,
		qtyPerContract: qtyPerContract, maxContracts: maxContracts,
	}, nil
}

func (m *optionsMarketMaker) Init(ctx context.Context, deps Deps) error {
	// Reconcile against the engine rather than trust a persisted snapshot,
	// exactly as marketMaker.Init does — a manual restart must never assume
	// stale order IDs are still live.
	if err := m.cancelAllTracked(ctx, deps); err != nil {
		return err
	}
	m.state.InitDone = true
	m.state.RealizedPnL = "0" // premium capture: net credits received minus debits paid, see applyFill
	return nil
}

func (m *optionsMarketMaker) OnStop(ctx context.Context, deps Deps) error {
	return m.cancelAllTracked(ctx, deps)
}

func (m *optionsMarketMaker) Snapshot() State { return *m.state }
func (m *optionsMarketMaker) Restore(s State) { m.state = &s }

func (m *optionsMarketMaker) OnTick(ctx context.Context, deps Deps) error {
	// Reconcile fills first so inventory-based skew reflects reality before
	// this tick's quotes go out.
	if err := m.detectFills(ctx, deps); err != nil {
		slog.Warn("options mm fill-detect failed", "underlying", m.underlying, "error", err)
	}

	chain, err := deps.Engine.OptionChain(ctx, m.underlying)
	if err != nil {
		// No chain (underlying not configured for options, or the engine
		// endpoint errored) — go flat rather than quote against stale data.
		slog.Warn("options mm chain fetch failed", "underlying", m.underlying, "error", err)
		return m.cancelAllTracked(ctx, deps)
	}
	if len(chain) == 0 {
		return m.cancelAllTracked(ctx, deps)
	}

	// Stable order (by expiry then strike then type) so maxContracts always
	// trims the same tail rather than a different random subset each tick.
	sort.Slice(chain, func(i, j int) bool {
		if chain[i].Expiry != chain[j].Expiry {
			return chain[i].Expiry < chain[j].Expiry
		}
		if chain[i].Strike != chain[j].Strike {
			return chain[i].Strike < chain[j].Strike
		}
		return chain[i].OptionType < chain[j].OptionType
	})
	if m.maxContracts > 0 && len(chain) > m.maxContracts {
		chain = chain[:m.maxContracts]
	}

	wanted := make(map[string]bool, len(chain)*2) // order IDs this tick intends to keep; anything else tracked gets cancelled
	netDelta := m.netDelta(chain)
	tenK := decimal.NewFromInt(10000)

	// Budget enforcement. investment is this desk's total quote budget (its
	// config label says exactly that: "Total quote budget backing all quoted
	// contracts") — but until now it was validated in the constructor, stored
	// on the struct, and then never read again. The desk quoted the ENTIRE
	// chain, both sides, at full qtyPerContract with no cap whatsoever: a
	// 20-contract chain meant 40 orders whose combined collateral could run
	// far past what the desk was actually funded with, and the only thing
	// stopping it was the engine rejecting individual orders once the wallet
	// ran dry — i.e. the budget was enforced by running out of money, at an
	// arbitrary point in the chain, rather than by this strategy.
	//
	// Cost model per quote, deliberately conservative (never under-estimates
	// what the engine will actually reserve, so the desk stops short of its
	// funding rather than over-committing and getting partial rejections):
	//   BUY  — pays premium: price * qty.
	//   SELL — writing an option is collateralized against the strike, not
	//          the premium received, so charge strike * qty. This mirrors
	//          risk.shortOptionMargin's conservative worst case; the engine's
	//          margin-floor model can only ever require LESS than this, so
	//          budgeting at the ceiling is safe in the direction that matters.
	//
	// Budget off the live quote balance when it's readable, for the same
	// self-correcting reason marketMaker does (investment is a funding-time
	// snapshot that real trading drifts away from and never updates).
	budget := m.investment
	if bal, err := deps.Engine.Balance(ctx, deps.Account, m.quoteAsset); err == nil && bal.Balance.IsPositive() {
		budget = bal.Balance
	}
	spent := decimal.Zero

	for _, c := range chain {
		fair, err := decimal.NewFromString(c.Mid)
		if err != nil || !fair.IsPositive() {
			continue
		}
		// Skew shifts both quotes the same direction, leaning the desk's own
		// price away from further building the side of inventory it's
		// already net long/short in — the same purpose marketMaker's
		// maxInventory cap serves, expressed as a price nudge instead of a
		// hard stop so the desk keeps two-sided liquidity even while skewed.
		skewFactor := decimal.NewFromInt(1).Add(netDelta.Mul(m.skewPerDelta).Div(tenK))
		skewedFair := fair.Mul(skewFactor)
		if skewedFair.IsNegative() {
			skewedFair = decimal.Zero
		}
		halfSpread := skewedFair.Mul(m.spreadBps).Div(tenK)
		bidPrice := decimal.Max(skewedFair.Sub(halfSpread), decimal.NewFromFloat(0.0001))
		askPrice := skewedFair.Add(halfSpread)

		// Charge each side against the budget before placing it, and skip the
		// side (not the whole contract) once it no longer fits — skipping only
		// what doesn't fit keeps the desk quoting the cheaper side of deeper
		// strikes instead of going dark on everything past the cut-off.
		buyCost := quoteCost("BUY", bidPrice, c.Strike, m.qtyPerContract)
		if spent.Add(buyCost).LessThanOrEqual(budget) {
			if id := m.ensureQuote(ctx, deps, c.Symbol, "BUY", bidPrice); id != "" {
				wanted[id] = true
				spent = spent.Add(buyCost)
			}
		}

		sellCost := quoteCost("SELL", askPrice, c.Strike, m.qtyPerContract)
		if spent.Add(sellCost).LessThanOrEqual(budget) {
			if id := m.ensureQuote(ctx, deps, c.Symbol, "SELL", askPrice); id != "" {
				wanted[id] = true
				spent = spent.Add(sellCost)
			}
		}
	}

	// Cancel anything tracked that this tick did not re-affirm: a contract
	// that fell off the chain (expired, or trimmed by maxContracts) still
	// has a resting quote until this cleans it up.
	for id, ref := range m.state.OpenOrders {
		if wanted[id] {
			continue
		}
		m.cancelOne(ctx, deps, id, ref)
	}
	return nil
}

// quoteCost is what one option quote consumes of the desk's quote budget.
// Extracted as a pure function so the budget rule is directly testable
// without standing up an engine (Deps.Engine is a concrete client, not an
// interface, so OnTick itself isn't unit-testable).
//
// BUY pays the premium. SELL is writing an option, which is collateralized
// against the STRIKE rather than the premium received — charging the premium
// there would wildly under-count the desk's real commitment (a $500 premium
// against a $60,000 strike). This mirrors risk.shortOptionMargin's
// conservative worst case; the engine's margin-floor model can only ever
// require less, so budgeting at the ceiling errs toward under-quoting rather
// than over-committing. A malformed or non-positive strike falls back to the
// premium notional so a bad chain entry is never treated as free.
func quoteCost(side string, price decimal.Decimal, strikeStr string, qty decimal.Decimal) decimal.Decimal {
	if side == "SELL" {
		if strike, err := decimal.NewFromString(strikeStr); err == nil && strike.IsPositive() {
			return strike.Mul(qty)
		}
	}
	return price.Mul(qty)
}

// ensureQuote places a fresh quote for (symbol, side) if one is not already
// resting near the target price, replacing (cancel + re-place) when the
// tracked quote has drifted beyond half its own spread from the new target —
// avoids re-submitting on every tick's tiny theoretical-price jitter while
// still tracking real fair-value moves. Returns the resting order ID, or ""
// if placement failed (the contract is simply left unquoted this tick; the
// next tick retries).
func (m *optionsMarketMaker) ensureQuote(ctx context.Context, deps Deps, symbol, side string, target decimal.Decimal) string {
	key := symbol + ":" + side
	if ref, ok := m.state.OpenOrders[key]; ok {
		existing := dec(ref.Price)
		if existing.IsPositive() {
			driftBps := target.Sub(existing).Abs().Div(existing).Mul(decimal.NewFromInt(10000))
			if driftBps.LessThan(m.spreadBps.Div(decimal.NewFromInt(2))) {
				return ref.OrderID // close enough; leave it resting
			}
		}
		m.cancelOne(ctx, deps, key, ref)
	}
	resp, err := deps.Engine.SubmitOrder(ctx, deps.Account, symbol, string(models.Options), side, "LIMIT", target, m.qtyPerContract, 0, "")
	if err != nil {
		slog.Warn("options mm place failed", "symbol", symbol, "side", side, "error", err)
		return ""
	}
	m.state.OpenOrders[key] = OrderRef{OrderID: resp.OrderID, Side: side, Price: target.String(), Qty: m.qtyPerContract.String(), Kind: symbol}
	return resp.OrderID
}

// cancelOne cancels one tracked quote by its map key (symbol+":"+side),
// accounting any fill that landed in the instant before cancellation via the
// cancel response's own final Filled — CancelOrder returns OrderResponse
// (Filled, Status), the same shape /order returns, so no separate status
// lookup is needed for the common cancel-then-forget path.
func (m *optionsMarketMaker) cancelOne(ctx context.Context, deps Deps, key string, ref OrderRef) {
	// ref.Kind carries the contract's own instrument symbol (see ensureQuote)
	// since the map key is "symbol:side", not the bare instrument symbol
	// CancelOrder needs.
	symbol := ref.Kind
	resp, err := deps.Engine.CancelOrder(ctx, deps.Account, symbol, string(models.Options), ref.OrderID)
	if err == nil {
		m.applyFillIfAny(&ref, dec(resp.Filled))
	} else {
		slog.Warn("options mm cancel failed", "symbol", symbol, "order", ref.OrderID, "error", err)
	}
	delete(m.state.OpenOrders, key)
}

// cancelAllTracked cancels every quote this strategy instance currently
// tracks (used by Init/OnStop/the no-chain path), rather than a single
// ladder-clear call — see the type doc comment on why this desk has no such
// single call to make.
func (m *optionsMarketMaker) cancelAllTracked(ctx context.Context, deps Deps) error {
	for key, ref := range m.state.OpenOrders {
		m.cancelOne(ctx, deps, key, ref)
	}
	return nil
}

// detectFills reconciles every tracked quote against the engine's
// authoritative order state in one /orders poll (across ALL the desk's
// resting orders, spanning every contract's separate order book at once —
// unlike marketMaker, which only ever has one symbol to check).
func (m *optionsMarketMaker) detectFills(ctx context.Context, deps Deps) error {
	open, err := deps.Engine.OpenOrders(ctx, deps.Account)
	if err != nil {
		return err
	}
	restingFilled := map[string]string{} // order ID -> cumulative filled qty
	for _, o := range open {
		restingFilled[o.ID] = o.Filled
	}
	for key, ref := range m.state.OpenOrders {
		if filledStr, ok := restingFilled[ref.OrderID]; ok {
			r := ref
			m.applyFillIfAny(&r, dec(filledStr))
			m.state.OpenOrders[key] = r
			continue
		}
		// Left the book — resolve its true terminal fill via order/status,
		// exactly as marketMaker.detectFills does, rather than assume a
		// vanished quote filled in full.
		st, err := deps.Engine.OrderStatusByID(ctx, ref.Kind, string(models.Options), ref.OrderID)
		if err != nil {
			slog.Warn("options mm order-status lookup failed", "symbol", ref.Kind, "order", ref.OrderID, "error", err)
			continue
		}
		if !st.Found {
			continue // durable record not yet caught up; retry next tick
		}
		r := ref
		m.applyFillIfAny(&r, dec(st.Filled))
		delete(m.state.OpenOrders, key)
	}
	return nil
}

// applyFillIfAny accounts the newly-filled delta of a tracked quote as
// premium capture: this desk is the writer on a filled SELL (receives
// premium) and the buyer on a filled BUY (pays premium) — realized P/L is
// simply net premium received minus paid, since a market-making desk here is
// not trying to hold directional option positions to expiry, only capture
// the bid/ask spread on flow. This deliberately tracks AppliedFilled/PnL
// directly rather than going through State.applyFillDelta/applySellFill: those
// shared helpers implement an avg-cost-basis model for a single directional
// BaseHeld position (grid/DCA/TWAP's use case), which does not apply here —
// this strategy holds simultaneous positions across many independent
// contracts and its P/L is spread capture, not cost-basis-vs-mark. Reusing
// them would silently fold premium flow into a BaseHeld/AvgEntry number that
// means nothing for this strategy.
//
// It also updates ContractPositions: a filled BUY increases this desk's
// signed holding in ref.Kind (the instrument symbol), a filled SELL
// decreases it — feeding netDelta's inventory-skew calculation.
func (m *optionsMarketMaker) applyFillIfAny(ref *OrderRef, filled decimal.Decimal) {
	applied := dec(ref.AppliedFilled)
	delta := filled.Sub(applied)
	if !delta.IsPositive() {
		return
	}
	ref.AppliedFilled = filled.String()
	notional := delta.Mul(dec(ref.Price))
	pnl := dec(m.state.RealizedPnL)
	if ref.Side == "SELL" {
		pnl = pnl.Add(notional)
	} else {
		pnl = pnl.Sub(notional)
	}
	m.state.RealizedPnL = pnl.String()

	if m.state.ContractPositions == nil {
		m.state.ContractPositions = map[string]string{}
	}
	held := dec(m.state.ContractPositions[ref.Kind])
	if ref.Side == "BUY" {
		held = held.Add(delta)
	} else {
		held = held.Sub(delta)
	}
	if held.IsZero() {
		delete(m.state.ContractPositions, ref.Kind)
	} else {
		m.state.ContractPositions[ref.Kind] = held.String()
	}

	m.state.recordTrade(time.Now()) // increments MatchedTrades and the 24h trade-time window
}

// netDelta approximates the desk's aggregate directional exposure across all
// quoted contracts: for each contract this desk currently holds a signed
// position in (State.ContractPositions, updated by applyFillIfAny on every
// fill), multiply the held size by the chain's engine-reported Greeks delta
// for that same contract and sum. A desk net long calls and short puts (both
// bullish) sums to a positive number; being flat or evenly hedged sums to
// ~zero. OnTick's skew then leans BOTH quotes on every contract away from
// this direction, discouraging further accumulation of the side the desk is
// already exposed to — the options equivalent of marketMaker's maxInventory
// cap, expressed as a continuous price nudge rather than a hard stop.
//
// Deliberately O(positions), not O(chain): most ticks hold positions in a
// small fraction of the full listed chain, so this walks the (usually much
// smaller) ContractPositions map and looks up each one's delta from chain,
// rather than the reverse.
func (m *optionsMarketMaker) netDelta(chain []engine.OptionChainEntry) decimal.Decimal {
	if len(m.state.ContractPositions) == 0 {
		return decimal.Zero
	}
	deltaBySymbol := make(map[string]float64, len(chain))
	for _, c := range chain {
		deltaBySymbol[c.Symbol] = c.Delta
	}
	total := decimal.Zero
	for symbol, heldStr := range m.state.ContractPositions {
		held := dec(heldStr)
		if held.IsZero() {
			continue
		}
		d, ok := deltaBySymbol[symbol]
		if !ok {
			continue // contract fell off the chain (expired) — its delta no longer matters
		}
		total = total.Add(held.Mul(decimal.NewFromFloat(d)))
	}
	return total
}
