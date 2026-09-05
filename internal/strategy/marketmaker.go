package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/dex/bots/internal/engine"
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
	quoteAsset   string
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
	requoteBps           decimal.Decimal
	lastMid              decimal.Decimal
	lastIndexTimestampMs int64
	// tick/lot are the engine's price/qty granularity for this symbol. Quotes are
	// snapped to them before submission — the engine rejects orders that aren't a
	// multiple of tick size / lot size. Seeded into config by the MM service at
	// desk creation (keys _tickSize / _lotSize); zero means "don't round".
	tick decimal.Decimal
	lot  decimal.Decimal
}

func mmParams() []models.TemplateParam {
	return []models.TemplateParam{
		{Key: "symbol", Label: "Trading Pair", Type: "text", Required: true, Default: "BTC-USDB", Help: "e.g. BTC-USDB"},
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
		quoteAsset: quoteAsset(bot.Market),
		market: bot.Market, investment: investment, spreadBps: spreadBps,
		levels: levels, levelStepBps: stepBps, maxInventory: maxInv,
		leverage: lev, marginMode: cfg(bot, "marginMode"), requoteBps: requoteBps,
		tick: decConfig(bot, "_tickSize"), lot: decConfig(bot, "_lotSize"),
	}, nil
}

// quoteAsset returns the desk's quote-leg currency: USDB for every market,
// spot and futures alike. Mirrors mm.collateralAsset (a different package,
// same rule) since the strategy needs to read its own quote-asset balance too.
func quoteAsset(market models.Market) string {
	return "USDB"
}

// decConfig parses a decimal config key, returning zero when absent or invalid.
func decConfig(bot *models.Bot, key string) decimal.Decimal {
	if d, err := decimal.NewFromString(cfg(bot, key)); err == nil {
		return d
	}
	return decimal.Zero
}

func (m *marketMaker) Init(ctx context.Context, deps Deps) error {
	// A stopped bot may have persisted order IDs after its orders were already
	// cancelled (or may have live quotes left by an interrupted shutdown). Do
	// not trust that snapshot on a manual restart: reconcile against the engine
	// and begin with a known-empty ladder.
	if err := m.cancelWalletQuotes(ctx, deps); err != nil {
		return err
	}
	// A spot desk's inventory is funded outside the order stream (the admin
	// deposits actual base and quote amounts independently — see mm.Service).
	// Never trust a persisted pre-restart baseline for it: it can belong to a
	// prior funding cycle. Rebuild both legs from the authoritative engine
	// balances before any new quotes are allowed.
	//
	// P/L for a market maker is deliberately quantity-based, not mark-to-
	// market: holding the same 1000 BTC after the index moves from $100k to
	// $120k is not a $20M profit, nothing was bought or sold. The only real
	// profit is ending up with MORE of an asset than this run started with
	// (spread capture), or less (a loss) — independent of price. So Init's
	// job is just to snapshot "what do we hold right now" as the zero point;
	// sampleEquity below diffs current holdings against this snapshot.
	if m.market == models.Spot {
		baseBal, err := deps.Engine.Balance(ctx, deps.Account, m.base)
		if err != nil {
			return fmt.Errorf("read base inventory baseline: %w", err)
		}
		quoteBal, err := deps.Engine.Balance(ctx, deps.Account, m.quoteAsset)
		if err != nil {
			return fmt.Errorf("read quote inventory baseline: %w", err)
		}
		m.state.BaseHeld = baseBal.Balance.String()
		m.state.QuoteHeld = quoteBal.Balance.String()
		m.state.BaseAtInit = baseBal.Balance.String()
		m.state.QuoteAtInit = quoteBal.Balance.String()
		m.state.RealizedPnL = "0"
		m.state.MatchedTrades = 0
		m.state.TradeTimes = nil
		m.state.Equity = nil
	}
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

	// 3. Every fresh external publication is a complete new quote set.  This
	// deliberately does not use the engine midpoint or a drift threshold: the
	// price-fetcher timestamp is the authoritative cadence.
	if len(m.state.OpenOrders) == 0 || (idx.TimestampMs > 0 && idx.TimestampMs != m.lastIndexTimestampMs) {
		if err := m.requote(ctx, deps, mid, idx.TimestampMs); err != nil {
			slog.Warn("mm requote failed", "symbol", m.symbol, "error", err)
		}
	}
	m.sampleEquity(mid)
	return nil
}

func (m *marketMaker) OnStop(ctx context.Context, deps Deps) error {
	return m.cancelWalletQuotes(ctx, deps)
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
func (m *marketMaker) requote(ctx context.Context, deps Deps, mid decimal.Decimal, timestampMs int64) error {
	// The engine rejects the WHOLE replacement ladder if any single level
	// crosses the live book (ReplaceMarketMakerLadder is all-or-nothing by
	// design — see its comment in matching-engine's reqReplaceAccountOrders —
	// so a partial ladder is never installed by accident). mid can drift
	// between when this tick sampled the index and when the request reaches
	// the engine; under fast movement the tightest level (spreadBps, the
	// smallest offset) is the one most likely to have been crossed by then.
	// Previously that meant the desk went dark on BOTH sides until the next
	// index publication happened to produce a wide-enough ladder — real gaps
	// of zero liquidity were observed on live symbols under load. Retrying
	// once with a wider spread clears a small amount of drift without
	// touching the all-or-nothing safety net itself (a still-crossing wider
	// ladder is correctly rejected again and the desk goes dark, same as
	// before — this only recovers the common case of a near-miss).
	return m.requoteWithSpread(ctx, deps, mid, timestampMs, m.spreadBps, true)
}

// requoteWithSpread builds and submits one ladder at the given half-spread.
// allowRetry gates the single widen-and-retry pass described above, so the
// retry attempt itself can't recurse.
func (m *marketMaker) requoteWithSpread(ctx context.Context, deps Deps, mid decimal.Decimal, timestampMs int64, spreadBps decimal.Decimal, allowRetry bool) error {
	held := dec(m.state.BaseHeld)
	tenK := decimal.NewFromInt(10000)
	numLevels := decimal.NewFromInt(int64(m.levels))
	// Per-level quote notional: split the budget evenly across all levels/side.
	//
	// Futures BUY and SELL both margin against the same quote currency, so
	// the investment budget must be split in half between the two sides
	// before dividing across levels — sizing each side off the FULL
	// investment independently (the old behavior) asked the engine to
	// reserve investment worth of margin for the buy side AND investment
	// worth for the sell side, i.e. 2x investment total across a full
	// two-sided ladder. That 2x mismatch is invisible at desk creation
	// (nothing checks it there) and only surfaces once quoting actually
	// starts, as every requote failing "insufficient balance to lock"
	// forever — the desk was always funded for exactly half of what its own
	// ladder asked the engine to reserve.
	//
	// Spot does NOT have this problem: its BUY side draws from quote_amount
	// (USDB/USDT) and its SELL side is capped by actual held BASE inventory
	// (see sellPerLevel below) — two independent currencies/pools, so the
	// full investment is correctly available to size the buy side alone.
	perLevel := m.investment.Div(numLevels)
	if m.market == models.Futures {
		// Budget off the account's LIVE quote balance, not the static
		// investment config. investment is a snapshot from whenever the desk
		// was funded/last recredited; real trading — fees, realized PnL on
		// closed legs, a partial liquidation — moves the actual balance both
		// up and down from then on, and investment never follows it. Once
		// real drift exceeds even a generous fixed-percentage buffer on top
		// of the stale number, every requote fails "insufficient balance to
		// lock" forever (the account HAS the room, investment just doesn't
		// know about it any more). Reading the true current balance here
		// keeps this self-correcting regardless of how far reality has
		// moved from the number the desk happened to be funded with.
		//
		// This is the total balance, not Available: the old ladder's
		// reservation is about to be released and replaced by this same
		// call, so what matters is what will be free once that happens —
		// which is the full balance, not balance-minus-the-old-lock.
		budget := m.investment
		if bal, err := deps.Engine.Balance(ctx, deps.Account, m.quoteAsset); err == nil && bal.Balance.IsPositive() {
			budget = bal.Balance
		}
		// futuresReserveBps carves out the same kind of safety margin spot's
		// sell side reserves (see sellReserveBps below): even sizing off the
		// live balance, a fill landing mid-requote (detectFills only
		// reconciles on the NEXT tick) can still nudge it a hair past exact.
		futuresReserveBps := decimal.NewFromInt(20) // 0.2%
		budget = budget.Mul(tenK.Sub(futuresReserveBps)).Div(tenK)
		perLevel = budget.Div(decimal.NewFromInt(2)).Div(numLevels)
	}

	// SELL side sizing must be capped by actual held base inventory, not
	// re-derived from the quote-denominated investment budget. Sizing each
	// level independently as investment/levels/askPrice silently assumes
	// askPrice ≈ the index price at the moment the desk was funded; once the
	// index has moved (spot desks are funded once, at recredit, off a single
	// snapshot price) that assumption drifts and the ladder's total ask
	// quantity can exceed what's actually in the wallet, which the engine
	// then rejects wholesale (ReplaceMarketMakerLadder is all-or-nothing) on
	// every single tick until the price recovers. Splitting the held balance
	// itself evenly across levels keeps the ladder inside the wallet no
	// matter how far the index has moved since funding.
	//
	// sellReserveBps carves out a small safety margin (0.5%) rather than
	// sizing the ladder to sum to exactly 100% of held. A sell quote that
	// fills lands asynchronously (detectFills only reconciles it on the next
	// tick), so "held" here can be a few ticks stale relative to the wallet's
	// real-time balance. Quoting the literal last unit of inventory means any
	// fill in that window — or even lot-size snapping rounding the wrong way
	// — tips the ladder's total ask notional a hair over what's actually free,
	// and every requote fails "insufficient balance to lock" until a BUY fill
	// happens to replenish it. A standing buffer absorbs that race instead of
	// depending on exact-balance timing.
	sellReserveBps := decimal.NewFromInt(50) // 0.5%
	sellPerLevel := decimal.Zero
	if m.market == models.Spot && held.IsPositive() {
		sellable := held.Mul(tenK.Sub(sellReserveBps)).Div(tenK)
		sellPerLevel = sellable.Div(numLevels)
	}

	quotes := make([]engine.MarketMakerQuote, 0, m.levels*2)
	for i := 0; i < m.levels; i++ {
		offBps := spreadBps.Add(m.levelStepBps.Mul(decimal.NewFromInt(int64(i))))
		off := offBps.Div(tenK)
		bidPrice := m.snapPrice(mid.Mul(decimal.NewFromInt(1).Sub(off)), false)
		askPrice := m.snapPrice(mid.Mul(decimal.NewFromInt(1).Add(off)), true)

		// BUY side — skip if inventory is already at/over the cap.
		if m.maxInventory.IsZero() || held.LessThan(m.maxInventory) {
			if qty := m.snapQty(qtyFor(perLevel, bidPrice)); qty.IsPositive() {
				quotes = append(quotes, engine.MarketMakerQuote{Side: "BUY", Price: bidPrice.String(), Qty: qty.String()})
			}
		}
		// SELL side. Futures desks have no base inventory to run out of (the
		// engine margins the position instead), so they keep sizing off the
		// quote budget. Spot desks size off actual held base, capped above.
		canSell := m.market == models.Futures || held.IsPositive()
		if canSell && (m.maxInventory.IsZero() || held.GreaterThan(m.maxInventory.Neg())) {
			var askQty decimal.Decimal
			if m.market == models.Spot {
				askQty = sellPerLevel
			} else {
				askQty = qtyFor(perLevel, askPrice)
			}
			if qty := m.snapQty(askQty); qty.IsPositive() {
				quotes = append(quotes, engine.MarketMakerQuote{Side: "SELL", Price: askPrice.String(), Qty: qty.String()})
			}
		}
	}
	if len(quotes) != m.levels*2 {
		return fmt.Errorf("cannot form complete two-sided ladder: got %d quotes", len(quotes))
	}
	if timestampMs == 0 {
		timestampMs = time.Now().UnixMilli()
	}
	resp, err := deps.Engine.ReplaceMarketMakerLadder(ctx, deps.Account, m.symbol, string(m.market), mid, timestampMs, quotes)
	if err != nil {
		// "did not rest" means mid drifted enough between sampling it and the
		// engine processing this request that a level now crosses the live
		// book; the engine correctly refused to install any partial ladder.
		// One retry at double the spread recovers from ordinary drift without
		// weakening that safety net — a level that still crosses at 2x spread
		// is a real, larger move, and the second failure is left to the next
		// tick exactly as before.
		if allowRetry && strings.Contains(err.Error(), "did not rest") {
			return m.requoteWithSpread(ctx, deps, mid, timestampMs, spreadBps.Mul(decimal.NewFromInt(2)), false)
		}
		return err
	}
	// A resting order can fill — fully or partially — in the instant before
	// this same replace call cancels it. detectFills reconciles fills for
	// every OTHER tick via a live /orders poll, but an order this replace
	// itself just tore down never appears in such a poll again; resp.Removed
	// is the ONLY place its true final Filled ever surfaces. Skipping this
	// would silently lose that fill forever — real inventory moves, the
	// strategy's BaseHeld never catches up, and every later requote asks the
	// engine to lock more than is actually left (see engine's MMReplaceResponse
	// doc comment).
	for _, o := range resp.Removed {
		ref, ok := m.state.OpenOrders[o.ID]
		if !ok {
			continue
		}
		price := dec(ref.Price)
		delta := m.state.applyFillDelta(&ref, dec(o.Filled), price)
		if delta.IsPositive() {
			m.applyQuoteDelta(ref.Side, delta, price)
			m.state.recordTrade(deps.MD.UpdatedAt)
		}
	}
	m.state.OpenOrders = make(map[string]OrderRef, len(resp.Orders))
	for _, o := range resp.Orders {
		m.state.OpenOrders[o.ID] = OrderRef{OrderID: o.ID, Side: o.Side, Price: o.Price, Qty: o.Qty, Level: -1, Kind: "mm"}
	}
	m.lastMid = mid
	m.lastIndexTimestampMs = timestampMs
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
	resp, err := deps.Engine.ClearMarketMakerLadder(ctx, deps.Account, m.symbol, string(m.market))
	if err != nil {
		return err
	}
	// Same reconciliation requote does: a resting order can fill in the
	// instant before this same call cancels it, and Removed is the only
	// place that fill's true final state ever surfaces (see requote's
	// comment on resp.Removed).
	for _, o := range resp.Removed {
		ref, ok := m.state.OpenOrders[o.ID]
		if !ok {
			continue
		}
		price := dec(ref.Price)
		delta := m.state.applyFillDelta(&ref, dec(o.Filled), price)
		if delta.IsPositive() {
			m.applyQuoteDelta(ref.Side, delta, price)
			m.state.recordTrade(deps.MD.UpdatedAt)
		}
	}
	m.state.OpenOrders = map[string]OrderRef{}
	return nil
}

// cancelWalletQuotes cancels every live quote for this MM account and symbol,
// rather than relying solely on the persisted OpenOrders map. This makes a
// manual stop/start safe even if the process stopped between an engine change
// and the next state persistence.
func (m *marketMaker) cancelWalletQuotes(ctx context.Context, deps Deps) error {
	return m.cancelAll(ctx, deps)
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
			delta := m.state.applyFillDelta(&r, dec(filledStr), price)
			if delta.IsPositive() {
				m.applyQuoteDelta(r.Side, delta, price)
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
		delta := m.state.applyFillDelta(&r, dec(st.Filled), price)
		if delta.IsPositive() {
			m.applyQuoteDelta(r.Side, delta, price)
			m.state.recordTrade(deps.MD.UpdatedAt)
		}
		delete(m.state.OpenOrders, id)
	}
	return nil
}

// applyQuoteDelta keeps QuoteHeld (the desk's actual quote-asset balance, e.g.
// USDT) in step with a fill, mirroring what applyBuyFill/applySellFill just
// did to BaseHeld: a BUY spends qty*price quote to gain qty base; a SELL
// gains qty*price quote for the qty base given up. This is the quote-side
// half of the quantity-based P/L model (see sampleEquity) — BaseHeld already
// tracks the base side via the shared avg-cost helpers.
func (m *marketMaker) applyQuoteDelta(side string, qty, price decimal.Decimal) {
	notional := qty.Mul(price)
	quoteHeld := dec(m.state.QuoteHeld)
	if side == "BUY" {
		m.state.QuoteHeld = quoteHeld.Sub(notional).String()
	} else {
		m.state.QuoteHeld = quoteHeld.Add(notional).String()
	}
}

// sampleEquity records equity for the drawdown/ROI curve, using the
// quantity-based P/L model: profit is holding MORE of an asset than this run
// started with (spread capture), not the index price moving. The base-asset
// delta since Init is valued at the current price only to express it in one
// number alongside the quote-asset delta — a real price move with unchanged
// holdings contributes exactly zero, unlike the old mark-to-market model.
func (m *marketMaker) sampleEquity(mid decimal.Decimal) {
	m.state.pushEquity(time.Now(), m.netPnL(mid))
}

// netPnL is (base held - base at Init) valued at the current price, plus
// (quote held - quote at Init). Both deltas are zero until the desk's own
// resting orders actually fill, so a bare index-price move never moves this
// number.
func (m *marketMaker) netPnL(mid decimal.Decimal) decimal.Decimal {
	baseDelta := dec(m.state.BaseHeld).Sub(dec(m.state.BaseAtInit))
	quoteDelta := dec(m.state.QuoteHeld).Sub(dec(m.state.QuoteAtInit))
	return baseDelta.Mul(mid).Add(quoteDelta)
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
