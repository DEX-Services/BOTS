// Package marketdata gives bot workers a single, shared view of each
// symbol's best bid/ask/mid. Instead of every bot polling the engine, one
// poller per (symbol, market) refreshes a snapshot and broadcasts a coalesced
// wake signal to every subscribed bot. With thousands of bots this keeps the
// number of engine connections O(symbols), not O(bots).
package marketdata

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dex/bots/internal/engine"
	"github.com/shopspring/decimal"
)

// Snapshot is an immutable point-in-time view of a symbol's top of book.
type Snapshot struct {
	Symbol    string
	Market    string
	Bid       decimal.Decimal
	Ask       decimal.Decimal
	Mid       decimal.Decimal
	Last      decimal.Decimal
	UpdatedAt time.Time
}

// Zero reports whether the snapshot has no usable price yet.
func (s Snapshot) Zero() bool { return s.Mid.IsZero() && s.Last.IsZero() }

// Hub fans out per-symbol market data to subscribers.
type Hub struct {
	engine *engine.Client
	poll   time.Duration
	mu     sync.Mutex
	feeds  map[string]*feed
}

// NewHub builds a Hub backed by the given engine client.
func NewHub(engineClient *engine.Client, poll time.Duration) *Hub {
	if poll <= 0 {
		poll = 800 * time.Millisecond
	}
	return &Hub{engine: engineClient, poll: poll, feeds: map[string]*feed{}}
}

type feed struct {
	hub   *Hub
	key   string
	snap  atomic.Pointer[Snapshot]
	subs  map[chan struct{}]struct{}
	subMu sync.Mutex
	stop  chan struct{}
	once  sync.Once
}

func key(symbol, market string) string { return strings.ToLower(symbol + "|" + market) }

// Snapshot returns the latest snapshot for a symbol/market (zero value if
// no data yet). It does not start a poller.
func (h *Hub) Snapshot(symbol, market string) Snapshot {
	h.mu.Lock()
	f := h.feeds[key(symbol, market)]
	h.mu.Unlock()
	if f == nil {
		return Snapshot{}
	}
	if p := f.snap.Load(); p != nil {
		return *p
	}
	return Snapshot{}
}

// Subscribe registers a bot for wake notifications on a symbol/market,
// starting the poller if needed. The returned channel receives a struct{}
// (coalesced, non-blocking) on every refresh. Call Unsubscribe to stop.
// Callers should only receive on the returned channel.
func (h *Hub) Subscribe(symbol, market string) chan struct{} {
	k := key(symbol, market)
	h.mu.Lock()
	f, ok := h.feeds[k]
	if !ok {
		f = &feed{hub: h, key: k, subs: map[chan struct{}]struct{}{}, stop: make(chan struct{})}
		h.feeds[k] = f
	}
	h.mu.Unlock()

	ch := make(chan struct{}, 1)
	f.subMu.Lock()
	f.subs[ch] = struct{}{}
	f.subMu.Unlock()

	f.once.Do(func() { go f.run(symbol, market) })
	return ch
}

// Unsubscribe removes a subscription and stops the poller when the last
// subscriber leaves.
func (h *Hub) Unsubscribe(symbol, market string, ch chan struct{}) {
	k := key(symbol, market)
	h.mu.Lock()
	f := h.feeds[k]
	h.mu.Unlock()
	if f == nil {
		return
	}
	f.subMu.Lock()
	delete(f.subs, ch)
	empty := len(f.subs) == 0
	f.subMu.Unlock()
	if empty {
		f.stop <- struct{}{}
	}
}

func (f *feed) run(symbol, market string) {
	ticker := time.NewTicker(f.hub.poll)
	defer ticker.Stop()
	// Refresh immediately so subscribers don't wait a full interval.
	f.refresh(symbol, market)
	for {
		select {
		case <-f.stop:
			return
		case <-ticker.C:
			f.refresh(symbol, market)
		}
	}
}

func (f *feed) refresh(symbol, market string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t, err := f.hub.engine.Ticker(ctx, symbol, market)
	if err != nil {
		slog.Warn("marketdata ticker failed", "symbol", symbol, "market", market, "error", err)
		return
	}
	last := t.Mid
	if last.IsZero() {
		last = t.Bid
	}
	snap := &Snapshot{
		Symbol: t.Symbol, Market: t.Market,
		Bid: t.Bid, Ask: t.Ask, Mid: t.Mid, Last: last,
		UpdatedAt: time.Now(),
	}
	f.snap.Store(snap)
	f.broadcast()
}

func (f *feed) broadcast() {
	f.subMu.Lock()
	subs := make([]chan struct{}, 0, len(f.subs))
	for ch := range f.subs {
		subs = append(subs, ch)
	}
	f.subMu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- struct{}{}:
		default: // subscriber is slow; it will see the latest on its next wake
		}
	}
}
