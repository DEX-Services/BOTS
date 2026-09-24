// Package runtime is the bot execution manager. One goroutine per running bot
// reacts to market-data wakes for its symbol and drives its strategy. State and
// stats are periodically persisted so a restart resumes every bot in place.
// The manager is the only thing that touches the in-memory worker map.
package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/dex/bots/internal/engine"
	"github.com/dex/bots/internal/index"
	"github.com/dex/bots/internal/marketdata"
	"github.com/dex/bots/internal/models"
	"github.com/dex/bots/internal/store"
	"github.com/dex/bots/internal/strategy"
	"github.com/shopspring/decimal"
)

const persistInterval = 3 * time.Second

// maxConsecutiveErrors stops a bot whose strategy keeps failing (e.g. every
// order rejected) instead of letting it retry indefinitely.
const maxConsecutiveErrors = 10

// tickErrorHaltPrefix marks a StatusError that came from repeated tick
// failures at RUNTIME, as opposed to one from a deterministic startup failure
// (an invalid strategy config, unreadable persisted state). StartAll uses it
// to decide whether a restart is worth attempting — see errorIsRetryable.
const tickErrorHaltPrefix = "stopped after repeated tick errors; last: "

// resumeFailedPrefix marks a StatusError recorded because StartAll could not
// bring a previously-running bot back up. Like a tick halt this is usually
// environmental (a dependency still coming up as the process boots), not a
// property of the bot, so it is treated as retryable on the NEXT start rather
// than disabling the bot for good — which matters most for market-maker desks,
// where a one-off failed resume must not take the desk permanently dark.
const resumeFailedPrefix = "could not be resumed after a restart; last: "

// Shutdown budget for StopAll. Stops run concurrently, so these bound the DB
// writes and the slowest single worker's teardown rather than their sum; the
// per-worker term keeps a large desk count from crowding out the tail.
const (
	stopAllBaseTimeout      = 10 * time.Second
	stopAllPerWorkerTimeout = 2 * time.Second
	stopAllMaxTimeout       = 60 * time.Second
)

// Manager owns all running bot workers.
type Manager struct {
	engine *engine.Client
	hub    *marketdata.Hub
	store  *store.Store
	// index reads the shared index price for market-maker bots. May be nil
	// when no Redis is configured; strategies that need it get a zero snapshot.
	index   *index.Reader
	mu      sync.Mutex
	workers map[string]*worker
	// starting holds bot IDs mid-Start so concurrent Start calls for the same
	// bot cannot both pass the duplicate check while store I/O is in flight.
	starting map[string]struct{}
}

// NewManager builds a manager. idx may be nil (index-dependent strategies then
// receive a stale/zero snapshot and refuse to quote).
func NewManager(engineClient *engine.Client, hub *marketdata.Hub, st *store.Store, idx *index.Reader) *Manager {
	return &Manager{engine: engineClient, hub: hub, store: st, index: idx, workers: map[string]*worker{}, starting: map[string]struct{}{}}
}

// StartAll resumes every bot that should be running after a restart.
//
// Two sources, deliberately: bots whose own status column still says running
// (any strategy — a user's TWAP/grid/DCA bot that was mid-run), plus every
// market-maker desk whose enabled flag is set. The second source is what makes
// a desk's resume survive a clean shutdown: StopAll marks the workers it stops
// as stopped, so a status-only resume brings back exactly the desks that were
// killed mid-flight and silently abandons the ones that exited properly —
// leaving an admin-enabled desk dark with nothing in the log to say so. The
// union is deduplicated (Start is a no-op on an already-running bot anyway, but
// the set keeps the log count honest).
func (m *Manager) StartAll(ctx context.Context) {
	ids := make([]string, 0, 16)
	seen := map[string]struct{}{}
	add := func(id string) {
		if _, dup := seen[id]; dup {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}

	bots, err := m.store.ListRunning(ctx)
	if err != nil {
		// Not fatal: the enabled-desk list below is an independent source, so
		// market makers can still come back even if this query fails.
		slog.Error("startup: list running bots failed", "error", err)
	}
	for i := range bots {
		add(bots[i].ID)
	}
	deskBots, err := m.store.EnabledDesks(ctx)
	if err != nil {
		slog.Error("startup: list enabled market-maker desks failed", "error", err)
	}
	for _, d := range deskBots {
		// An errored desk used to be skipped unconditionally, on the reasoning
		// that StatusError means a real, persistent failure and reviving it
		// every boot would crash-loop it. That holds for a DETERMINISTIC
		// failure — an invalid strategy config or corrupt persisted state will
		// fail identically on the next boot, so retrying is pointless noise.
		//
		// It does not hold for a tick-error halt, which is the far more common
		// case and is usually environmental rather than a property of the bot:
		// the matching engine restarting, the index price going stale, the
		// backend briefly unavailable. Ten consecutive failed ticks is only
		// ~10 seconds of an outage, so any dependency blip long enough to
		// notice halts every desk on the platform — and they then all stayed
		// dark until someone re-enabled each one by hand, even though the
		// condition that stopped them was already over. Observed exactly that
		// during this project's own restarts.
		//
		// So: retry once per process start for a runtime halt, skip a
		// deterministic one. The retry is bounded by construction — one
		// attempt per boot, and if the underlying problem really is permanent
		// the bot re-errors within ~10 more ticks and is skipped by the same
		// path on the next boot only if its recorded error changed shape.
		if d.Status == string(models.StatusError) {
			if !errorIsRetryable(d.Error) {
				slog.Warn("startup: enabled desk left stopped; its bot failed in a way a restart cannot fix",
					"bot", d.BotID, "symbol", d.Symbol, "error", d.Error)
				continue
			}
			slog.Info("startup: retrying enabled desk that halted on tick errors",
				"bot", d.BotID, "symbol", d.Symbol, "error", d.Error)
		}
		add(d.BotID)
	}

	var failed int
	for _, id := range ids {
		if err := m.Start(ctx, id); err != nil {
			failed++
			slog.Warn("startup: resume bot failed", "id", id, "error", err)
			// A bot that was running before the restart and could not be
			// resumed must not keep claiming status=running. Start only
			// updates the row for a few specific failures (an invalid config,
			// corrupt state); anything else — including a failed Init — left
			// the row untouched, so it still read "running" with no worker
			// behind it and no error to show. The user then saw a healthy
			// running bot whose real orders were resting unmanaged on the
			// engine, locks held, with nothing left to detect their fills or
			// cancel them. Recording the failure is what makes that state
			// visible and actionable (stop releases the orders).
			_ = m.store.UpdateStatus(ctx, id, models.StatusError,
				resumeFailedPrefix+err.Error())
		}
	}
	slog.Info("startup: resumed bots", "count", len(m.workers), "candidates", len(ids), "failed", failed)
}

// errorIsRetryable reports whether a bot's recorded StatusError message came
// from a runtime tick-error halt (worth one retry on the next process start)
// rather than a deterministic startup failure (an invalid strategy config or
// unreadable/corrupt persisted state, which will fail the same way every time
// and so must not be retried).
//
// Deliberately fails CLOSED: anything that doesn't carry the tick-halt prefix
// is treated as non-retryable, so a new error path added later is skipped and
// logged rather than silently crash-looped.
func errorIsRetryable(errMsg string) bool {
	return strings.HasPrefix(errMsg, tickErrorHaltPrefix) ||
		strings.HasPrefix(errMsg, resumeFailedPrefix)
}

// Start builds and runs a bot. Safe to call on an already-running bot.
// Concurrent Start calls for the same bot are serialized via the starting set:
// exactly one proceeds, the rest return immediately.
func (m *Manager) Start(ctx context.Context, botID string) error {
	m.mu.Lock()
	if _, ok := m.workers[botID]; ok {
		m.mu.Unlock()
		return nil // already running
	}
	if _, ok := m.starting[botID]; ok {
		m.mu.Unlock()
		return nil // another Start is already in flight
	}
	m.starting[botID] = struct{}{}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.starting, botID)
		m.mu.Unlock()
	}()

	bot, err := m.store.Get(ctx, botID)
	if err != nil {
		return err
	}
	strat, err := strategy.Build(bot)
	if err != nil {
		_ = m.store.UpdateStatus(ctx, botID, models.StatusError, err.Error())
		return err
	}
	// Restore persisted state (if any) into the strategy. A restore failure
	// means the persisted state is corrupt — surface it and refuse to run
	// rather than silently trading from a blank state.
	if len(bot.State) > 0 {
		var st strategy.State
		raw, merr := json.Marshal(bot.State)
		if merr != nil {
			_ = m.store.UpdateStatus(ctx, botID, models.StatusError, "persisted state unreadable: "+merr.Error())
			return merr
		}
		if uerr := json.Unmarshal(raw, &st); uerr != nil {
			_ = m.store.UpdateStatus(ctx, botID, models.StatusError, "persisted state corrupt: "+uerr.Error())
			return uerr
		}
		strat.Restore(st)
	}
	// Looked up once per Start, not per tick — lot/tick size never change for
	// a symbol mid-run, and this is a DB round-trip. A lookup failure (symbol
	// not found/inactive) leaves lot/tick at zero, which snapToLot treats as
	// "don't round" — acceptable for now since Build() above already
	// validates the symbol exists via other means for most strategies, but
	// worth revisiting if a genuinely-unlisted symbol ever reaches here.
	var lotSize, tickSize, maxQty decimal.Decimal
	if spec, serr := m.store.LookupSymbol(ctx, bot.Symbol, string(bot.Market)); serr == nil {
		lotSize, tickSize, maxQty = spec.Lot, spec.Tick, spec.MaxQuantity
	}
	wakeCh := m.hub.Subscribe(bot.Symbol, string(bot.Market))
	// Subscribe starts the symbol's poller on its own goroutine, so for the
	// first bot on a symbol the hub has NOTHING cached at this instant and
	// Snapshot returns the zero value. Strategies that need a price to seed
	// themselves (grid's Init refuses outright without a mid) then fail the
	// whole Start on what is purely a cold-cache timing artifact.
	//
	// That hurt worst exactly where it matters most — resuming after a crash.
	// StartAll runs seconds after boot, before any feed has warmed, so a
	// perfectly healthy user grid failed to resume with "no market data",
	// leaving its row at status=running with no worker and no error: the UI
	// showed it running while its real orders sat unmanaged on the engine,
	// their balance locks held, with nothing left to ever detect a fill or
	// cancel them. Confirmed live via kill -9.
	//
	// Waiting briefly for the first poll costs nothing in the warm case
	// (Snapshot returns immediately) and is bounded, so a genuinely dead
	// symbol still fails rather than hanging startup.
	md := m.awaitMarketData(ctx, bot.Symbol, string(bot.Market))
	deps := strategy.Deps{
		Engine: m.engine, Account: bot.UserID, Bot: bot,
		MD:    md,
		Index: m.indexSnapshot(ctx, bot.Symbol, bot.Config),
		Lot:   lotSize, Tick: tickSize, MaxQuantity: maxQty,
	}
	// Strategies may reconcile external state before they begin ticking. In
	// particular, a market maker clears stale persisted order IDs here so a
	// manual stop/start always resumes with a clean ladder.
	if err := strat.Init(ctx, deps); err != nil {
		m.hub.Unsubscribe(bot.Symbol, string(bot.Market), wakeCh)
		return fmt.Errorf("initialize bot: %w", err)
	}
	if err := m.store.MarkRunning(ctx, botID); err != nil {
		m.hub.Unsubscribe(bot.Symbol, string(bot.Market), wakeCh)
		return err
	}
	bot.Status = models.StatusRunning
	w := &worker{
		manager: m, bot: bot, strategy: strat,
		wakeCh: wakeCh,
		stopCh: make(chan struct{}), doneCh: make(chan struct{}),
		startedAt: time.Now(),
		lotSize:   lotSize, tickSize: tickSize,
	}
	m.mu.Lock()
	m.workers[botID] = w
	m.mu.Unlock()
	go w.run()
	slog.Info("bot started", "id", botID, "strategy", bot.Strategy, "symbol", bot.Symbol)
	return nil
}

// marketDataWarmup bounds how long Start waits for a freshly-subscribed symbol
// feed to publish its first snapshot. The hub polls the engine on its own
// interval, so this only needs to cover one poll plus the engine round trip;
// a symbol that cannot produce a price in this long is not one a bot should
// start trading on anyway, and the strategy's own Init gets to reject it.
const marketDataWarmup = 5 * time.Second

// awaitMarketData returns the symbol's snapshot, waiting up to
// marketDataWarmup for the first one when the feed was only just subscribed.
// Returns whatever it has when the budget expires (possibly still zero) and
// lets the caller's strategy decide whether that is fatal.
func (m *Manager) awaitMarketData(ctx context.Context, symbol, market string) marketdata.Snapshot {
	deadline := time.Now().Add(marketDataWarmup)
	for {
		if snap := m.hub.Snapshot(symbol, market); !snap.Zero() {
			return snap
		}
		if time.Now().After(deadline) {
			return m.hub.Snapshot(symbol, market)
		}
		select {
		case <-ctx.Done():
			return m.hub.Snapshot(symbol, market)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Stop cancels a bot's resting orders and stops its worker.
func (m *Manager) Stop(ctx context.Context, botID string) error {
	m.mu.Lock()
	w, ok := m.workers[botID]
	if ok {
		delete(m.workers, botID) // claim teardown while holding the lock
	}
	m.mu.Unlock()
	if !ok {
		// No live worker — but that does NOT mean there is nothing on the
		// exchange. A bot whose process was killed mid-run, or whose resume
		// failed, still has every order it placed resting on the engine with
		// its balance locks held, recorded only in the bot's persisted state.
		// Marking the row stopped and returning (as this used to) reported
		// success while leaving those orders live and unmanaged forever, with
		// no remaining way for the user to get that money back — confirmed
		// live: after a kill -9, stop returned 200 and the account's reserved
		// balance did not move at all.
		m.cancelPersistedOrders(ctx, botID)
		return m.store.MarkStopped(ctx, botID)
	}
	close(w.stopCh)
	<-w.doneCh
	m.hub.Unsubscribe(w.bot.Symbol, string(w.bot.Market), w.wakeCh)
	return m.store.MarkStopped(ctx, botID)
}

// cancelPersistedOrders cancels the orders recorded in a stopped bot's
// persisted state, for the case where no live worker exists to do it (the
// process was killed mid-run, or the bot failed to resume). Best-effort by
// design: it runs on the user-facing stop path, and an order that is already
// gone — filled, cancelled, or lost to an engine restart — simply errors
// harmlessly here. Failures are logged, never returned, so the caller still
// marks the bot stopped rather than leaving it stuck as running.
func (m *Manager) cancelPersistedOrders(ctx context.Context, botID string) {
	bot, err := m.store.Get(ctx, botID)
	if err != nil || len(bot.State) == 0 {
		return
	}
	raw, err := json.Marshal(bot.State)
	if err != nil {
		return
	}
	var st strategy.State
	if err := json.Unmarshal(raw, &st); err != nil {
		slog.Warn("orphan cleanup: unreadable persisted state", "id", botID, "error", err)
		return
	}
	for id := range st.OpenOrders {
		if _, err := m.engine.CancelOrder(ctx, bot.UserID, bot.Symbol, string(bot.Market), id); err != nil {
			slog.Warn("orphan cleanup: cancel failed", "id", botID, "order", id, "error", err)
			continue
		}
		slog.Info("orphan cleanup: cancelled order left by a stopped worker",
			"id", botID, "order", id, "symbol", bot.Symbol)
	}
}

// StopAll gracefully stops every worker (used on shutdown).
//
// Workers stop concurrently, and the budget scales with how many there are.
// Sequential stops sharing one flat 10s deadline did not survive the desk
// count growing: each Stop waits on its worker's own shutdown, which cancels
// that desk's resting quotes over HTTP, so the total ran past the deadline and
// the tail of the list was cut off — those bots stayed marked running with
// their quotes still on the book and their durable balance locks still held.
// Stopping in parallel makes the wall clock the slowest single desk instead of
// the sum of all of them.
func (m *Manager) StopAll() {
	m.mu.Lock()
	ids := make([]string, 0, len(m.workers))
	for id := range m.workers {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	budget := stopAllBaseTimeout + time.Duration(len(ids))*stopAllPerWorkerTimeout
	if budget > stopAllMaxTimeout {
		budget = stopAllMaxTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(botID string) {
			defer wg.Done()
			if err := m.Stop(ctx, botID); err != nil {
				slog.Warn("shutdown: stop bot failed", "id", botID, "error", err)
			}
		}(id)
	}
	wg.Wait()
}

// IsRunning reports whether a bot currently has a live worker.
func (m *Manager) IsRunning(botID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.workers[botID]
	return ok
}

type worker struct {
	manager   *Manager
	bot       *models.Bot
	strategy  strategy.Strategy
	wakeCh    chan struct{}
	stopCh    chan struct{}
	doneCh    chan struct{}
	startedAt time.Time
	// consecutiveErrors counts back-to-back OnTick failures; reset on success.
	consecutiveErrors int
	// lot/tick are the bot's symbol granularity, looked up once at Start and
	// reused for every Deps built during this run (see strategy.Deps.Lot's
	// doc comment for why every quantity-computing strategy needs this).
	lotSize, tickSize decimal.Decimal
}

func (w *worker) run() {
	defer close(w.doneCh)
	persist := time.NewTicker(persistInterval)
	defer persist.Stop()
	// The external index is updated independently from the engine's trade
	// stream. A one-second wake guarantees market makers react to each fresh
	// Price-Fetcher publication even while their own book has no trades.
	indexTick := time.NewTicker(time.Second)
	defer indexTick.Stop()
	ctx := context.Background()
	for {
		select {
		case <-w.stopCh:
			w.shutdown(ctx)
			return
		case <-w.wakeCh:
			if halted := w.tick(ctx); halted {
				w.shutdown(ctx)
				go w.manager.remove(w) // detach off the worker goroutine; Stop would deadlock here
				return
			}
		case <-indexTick.C:
			// options_market_maker has the same problem market_maker does —
			// worse, in fact: each option instrument has its own (mostly
			// idle) order book, so it can go many ticks between engine-side
			// trades on ANY contract. The 1s wake is this desk's only
			// reliable heartbeat; it doesn't use the index price directly
			// (it quotes off the engine's own theoretical /option-chain
			// fair value, which itself derives from the underlying's mark)
			// but still needs to run on a fixed cadence rather than waiting
			// for book activity that may never come.
			if w.bot.Strategy == "market_maker" || w.bot.Strategy == "options_market_maker" {
				if halted := w.tick(ctx); halted {
					w.shutdown(ctx)
					go w.manager.remove(w)
					return
				}
			}
		case <-persist.C:
			w.persist(ctx)
		}
	}
}

// remove unregisters a self-stopped worker (already shut down) from the
// manager and hub. Runs on its own goroutine, so taking the lock is safe.
// If Stop already claimed the worker (deleted it from the map), teardown is
// Stop's responsibility and remove does nothing.
func (m *Manager) remove(w *worker) {
	m.mu.Lock()
	cur, ok := m.workers[w.bot.ID]
	if ok && cur == w {
		delete(m.workers, w.bot.ID)
	}
	m.mu.Unlock()
	if ok && cur == w {
		m.hub.Unsubscribe(w.bot.Symbol, string(w.bot.Market), w.wakeCh)
	}
}

// indexSnapshot returns the current index-price snapshot for a symbol's base
// asset, or a zero (stale) snapshot when no index reader is configured. A bot
// config may pin the exact Redis ticker via "_indexTicker" — needed when the
// real key's casing doesn't survive baseAsset's uppercasing ("CrudeOIL-BI2XUSD"
// -> "CRUDEOIL", "AAPL.us-BI2XUSD" -> "AAPL.US", neither of which is a key
// Price-Fetcher ever writes). Without that override the lookup falls back to
// the pair-derived base asset ("BTC-BI2XUSD" -> "BTC").
func (m *Manager) indexSnapshot(ctx context.Context, symbol string, cfg map[string]string) index.Snapshot {
	if m.index == nil {
		return index.Snapshot{}
	}
	if ticker := strings.TrimSpace(cfg["_indexTicker"]); ticker != "" {
		return m.index.GetExact(ctx, ticker, time.Now().UnixMilli())
	}
	return m.index.Get(ctx, baseAsset(symbol), time.Now().UnixMilli())
}

// IndexSnapshot exposes the index snapshot for a bot's symbol to callers
// outside the runtime (e.g. the MM admin API showing a desk's index price).
// cfg is the bot's strategy config, consulted for the "_indexTicker" override
// exactly as the quoting path does — pass it so the admin view reads the same
// Redis key the desk actually quotes against, rather than reporting "no index"
// for the mixed-case tickers. Returns a zero (stale) snapshot when no index
// reader is configured.
func (m *Manager) IndexSnapshot(ctx context.Context, symbol string, cfg map[string]string) index.Snapshot {
	return m.indexSnapshot(ctx, symbol, cfg)
}

// IndexSnapshotExact looks up the index price for an EXACT asset ticker, with
// no pair-parsing and no case normalization — unlike IndexSnapshot (via
// baseAsset), which force-uppercases and is meant for crypto pairs like
// "BTC-USDC" -> "BTC". That uppercasing silently breaks any ticker whose
// real casing matters, e.g. price-fetcher's Live-Rates.com stock tickers are
// stored as "price:AAPL.us" (lowercase suffix, by that upstream's own
// case-sensitive contract — see Price-Fetcher/README.md) — a lookup for
// "AAPL.US" simply misses. Used by the /index/{base} HTTP handler, which
// takes a caller-supplied ticker that's already the exact key to look up,
// not a pair needing parsing.
func (m *Manager) IndexSnapshotExact(ctx context.Context, ticker string) index.Snapshot {
	if m.index == nil {
		return index.Snapshot{}
	}
	return m.index.GetExact(ctx, ticker, time.Now().UnixMilli())
}

// baseAsset extracts the base symbol from a pair like "BTC-USDC" -> "BTC".
func baseAsset(symbol string) string {
	s := strings.ToUpper(strings.TrimSpace(symbol))
	for _, sep := range []string{"-", "/", "_"} {
		if i := strings.Index(s, sep); i > 0 {
			return s[:i]
		}
	}
	return s
}

// tick runs one strategy iteration. Returns true when the bot has failed too
// many times in a row and must stop.
func (w *worker) tick(ctx context.Context) (halt bool) {
	md := w.manager.hub.Snapshot(w.bot.Symbol, string(w.bot.Market))
	deps := strategy.Deps{
		Engine: w.manager.engine, Account: w.bot.UserID,
		Bot: w.bot, MD: md, Index: w.manager.indexSnapshot(ctx, w.bot.Symbol, w.bot.Config),
		Lot: w.lotSize, Tick: w.tickSize,
	}
	if err := w.strategy.OnTick(ctx, deps); err != nil {
		w.consecutiveErrors++
		slog.Warn("bot tick error", "id", w.bot.ID, "strategy", w.bot.Strategy,
			"consecutive", w.consecutiveErrors, "error", err)
		if w.consecutiveErrors >= maxConsecutiveErrors {
			slog.Error("bot stopped after repeated errors", "id", w.bot.ID, "errors", w.consecutiveErrors)
			_ = w.manager.store.UpdateStatus(ctx, w.bot.ID, models.StatusError,
				tickErrorHaltPrefix+err.Error())
			return true
		}
		return false
	}
	w.consecutiveErrors = 0
	return false
}

func (w *worker) persist(ctx context.Context) {
	md := w.manager.hub.Snapshot(w.bot.Symbol, string(w.bot.Market))
	idx := w.manager.indexSnapshot(ctx, w.bot.Symbol, w.bot.Config)
	state := w.strategy.Snapshot()
	stats := computeStats(state, md, idx, w.bot, w.startedAt, time.Now())
	if err := w.manager.store.SaveState(ctx, w.bot.ID, state, stats); err != nil {
		slog.Warn("bot persist failed", "id", w.bot.ID, "error", err)
	}
}

func (w *worker) shutdown(ctx context.Context) {
	md := w.manager.hub.Snapshot(w.bot.Symbol, string(w.bot.Market))
	deps := strategy.Deps{
		Engine: w.manager.engine, Account: w.bot.UserID,
		Bot: w.bot, MD: md, Lot: w.lotSize, Tick: w.tickSize,
	}
	if err := w.strategy.OnStop(ctx, deps); err != nil {
		slog.Warn("bot on-stop error", "id", w.bot.ID, "error", err)
	}
	w.persist(ctx)
}

// computeStats derives the UI/marketplace metrics from strategy state + price.
//
// Unrealized P/L is marked against the external index price, not the engine's
// own order-book mid: right after an engine restart (or any moment the book
// is missing a best bid or ask) Ticker returns Mid=0, and marking held
// inventory against a zero price manufactures a phantom ~100% loss even
// though nothing was actually sold. The strategy already treats the index as
// the authoritative reference for quoting (see marketMaker's doc comment);
// stats need the same reference to stay consistent with reality. The book
// mid remains the fallback for strategies/symbols with no index feed.
//
// market_maker desks use a different P/L model than every other strategy
// here: quantity-based, not mark-to-market. A grid/DCA/TWAP bot's whole point
// is capturing price movement, so avg-cost-vs-current-price is the right
// measure for them. A market maker's profit is spread capture — ending up
// with a different quantity of an asset than it started this run with — and
// is independent of where the index price sits; see marketMaker.netPnL.
func computeStats(s strategy.State, md marketdata.Snapshot, idx index.Snapshot, bot *models.Bot, startedAt, now time.Time) models.Stats {
	mark := md.Mid
	if idx.Fresh && idx.Price.IsPositive() {
		mark = idx.Price
	}

	held := dec(s.BaseHeld)
	avg := dec(s.AvgEntry)

	var realized, unrealized decimal.Decimal
	if bot.Strategy == "market_maker" {
		baseDelta := held.Sub(dec(s.BaseAtInit))
		quoteDelta := dec(s.QuoteHeld).Sub(dec(s.QuoteAtInit))
		unrealized = baseDelta.Mul(mark).Add(quoteDelta)
		realized = decimal.Zero // folded into unrealized above; kept separate stat at 0 to avoid double count
	} else {
		realized = dec(s.RealizedPnL)
		unrealized = held.Mul(mark.Sub(avg))
	}
	net := realized.Add(unrealized)
	roi := decimal.Zero
	if inv := dec(bot.Investment); inv.IsPositive() {
		roi = net.Div(inv).Mul(decimal.NewFromInt(100))
	}
	stats := models.NewStats()
	stats.RealizedPnL = realized.String()
	stats.UnrealizedPnL = unrealized.String()
	stats.NetPnL = net.String()
	stats.ROI = roi.String()
	stats.RuntimeSec = int64(now.Sub(startedAt).Seconds())
	stats.MatchedTrades = s.MatchedTrades
	stats.Trades24h = s.Trades24h(now)
	stats.MaxDrawdownPct = s.MaxDrawdown().String()
	stats.BaseHeld = held.String()
	// market_maker never tracks a cost basis (see the doc comment above: its
	// P/L is quantity-based, not mark-to-market), so s.AvgEntry is always
	// the zero value here. Reporting that as "0" reads as "bought at price
	// zero" on an admin dashboard even while the desk holds real inventory —
	// leave it blank rather than assert a cost basis that was never tracked.
	if bot.Strategy != "market_maker" && bot.Strategy != "options_market_maker" {
		stats.AvgEntryPrice = avg.String()
	}
	return stats
}

func dec(s string) decimal.Decimal {
	d, _ := decimal.NewFromString(s)
	return d
}
