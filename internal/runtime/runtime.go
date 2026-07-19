// Package runtime is the bot execution manager. One goroutine per running bot
// reacts to market-data wakes for its symbol and drives its strategy. State and
// stats are periodically persisted so a restart resumes every bot in place.
// The manager is the only thing that touches the in-memory worker map.
package runtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/dex/bots/internal/engine"
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

// Manager owns all running bot workers.
type Manager struct {
	engine  *engine.Client
	hub     *marketdata.Hub
	store   *store.Store
	mu      sync.Mutex
	workers map[string]*worker
	// starting holds bot IDs mid-Start so concurrent Start calls for the same
	// bot cannot both pass the duplicate check while store I/O is in flight.
	starting map[string]struct{}
}

// NewManager builds a manager.
func NewManager(engineClient *engine.Client, hub *marketdata.Hub, st *store.Store) *Manager {
	return &Manager{engine: engineClient, hub: hub, store: st, workers: map[string]*worker{}, starting: map[string]struct{}{}}
}

// StartAll resumes every bot marked running in the database.
func (m *Manager) StartAll(ctx context.Context) {
	bots, err := m.store.ListRunning(ctx)
	if err != nil {
		slog.Error("startup: list running bots failed", "error", err)
		return
	}
	for i := range bots {
		if err := m.Start(ctx, bots[i].ID); err != nil {
			slog.Warn("startup: resume bot failed", "id", bots[i].ID, "error", err)
		}
	}
	slog.Info("startup: resumed bots", "count", len(m.workers))
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
	if err := m.store.MarkRunning(ctx, botID); err != nil {
		return err
	}
	bot.Status = models.StatusRunning

	w := &worker{
		manager: m, bot: bot, strategy: strat,
		wakeCh: m.hub.Subscribe(bot.Symbol, string(bot.Market)),
		stopCh: make(chan struct{}), doneCh: make(chan struct{}),
		startedAt: time.Now(),
	}
	m.mu.Lock()
	m.workers[botID] = w
	m.mu.Unlock()
	go w.run()
	slog.Info("bot started", "id", botID, "strategy", bot.Strategy, "symbol", bot.Symbol)
	return nil
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
		// Not running; just ensure the DB reflects stopped.
		return m.store.MarkStopped(ctx, botID)
	}
	close(w.stopCh)
	<-w.doneCh
	m.hub.Unsubscribe(w.bot.Symbol, string(w.bot.Market), w.wakeCh)
	return m.store.MarkStopped(ctx, botID)
}

// StopAll gracefully stops every worker (used on shutdown).
func (m *Manager) StopAll() {
	m.mu.Lock()
	ids := make([]string, 0, len(m.workers))
	for id := range m.workers {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, id := range ids {
		_ = m.Stop(ctx, id)
	}
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
}

func (w *worker) run() {
	defer close(w.doneCh)
	persist := time.NewTicker(persistInterval)
	defer persist.Stop()
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

// tick runs one strategy iteration. Returns true when the bot has failed too
// many times in a row and must stop.
func (w *worker) tick(ctx context.Context) (halt bool) {
	md := w.manager.hub.Snapshot(w.bot.Symbol, string(w.bot.Market))
	deps := strategy.Deps{
		Engine: w.manager.engine, Account: w.bot.WalletAddress,
		Bot: w.bot, MD: md,
	}
	if err := w.strategy.OnTick(ctx, deps); err != nil {
		w.consecutiveErrors++
		slog.Warn("bot tick error", "id", w.bot.ID, "strategy", w.bot.Strategy,
			"consecutive", w.consecutiveErrors, "error", err)
		if w.consecutiveErrors >= maxConsecutiveErrors {
			slog.Error("bot stopped after repeated errors", "id", w.bot.ID, "errors", w.consecutiveErrors)
			_ = w.manager.store.UpdateStatus(ctx, w.bot.ID, models.StatusError,
				"stopped after repeated tick errors; last: "+err.Error())
			return true
		}
		return false
	}
	w.consecutiveErrors = 0
	return false
}

func (w *worker) persist(ctx context.Context) {
	md := w.manager.hub.Snapshot(w.bot.Symbol, string(w.bot.Market))
	state := w.strategy.Snapshot()
	stats := computeStats(state, md, w.bot, w.startedAt, time.Now())
	if err := w.manager.store.SaveState(ctx, w.bot.ID, state, stats); err != nil {
		slog.Warn("bot persist failed", "id", w.bot.ID, "error", err)
	}
}

func (w *worker) shutdown(ctx context.Context) {
	md := w.manager.hub.Snapshot(w.bot.Symbol, string(w.bot.Market))
	deps := strategy.Deps{
		Engine: w.manager.engine, Account: w.bot.WalletAddress,
		Bot: w.bot, MD: md,
	}
	if err := w.strategy.OnStop(ctx, deps); err != nil {
		slog.Warn("bot on-stop error", "id", w.bot.ID, "error", err)
	}
	w.persist(ctx)
}

// computeStats derives the UI/marketplace metrics from strategy state + price.
func computeStats(s strategy.State, md marketdata.Snapshot, bot *models.Bot, startedAt, now time.Time) models.Stats {
	realized := dec(s.RealizedPnL)
	held := dec(s.BaseHeld)
	avg := dec(s.AvgEntry)
	unrealized := held.Mul(md.Mid.Sub(avg))
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
	stats.AvgEntryPrice = avg.String()
	return stats
}

func dec(s string) decimal.Decimal {
	d, _ := decimal.NewFromString(s)
	return d
}
