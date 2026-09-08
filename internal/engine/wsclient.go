package engine

import (
	"encoding/json"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WSEvent mirrors the matching engine's outbound event shape (see
// matching-engine/internal/models.Event). Only the fields consumers here
// actually use are decoded; everything else is ignored.
type WSEvent struct {
	Type           string   `json:"type"`
	Symbol         string   `json:"symbol"`
	Market         string   `json:"market"`
	SequenceNumber uint64   `json:"sequenceNumber"`
	Order          *WSOrder `json:"order"`
}

// WSOrder mirrors the fields of matching-engine's Order that fill accounting
// needs. AccountID lets subscribers filter to only their own orders — the
// hub broadcasts every account's events to every connected client, so this
// filtering happens client-side, same as the frontend's wsClient does for UI
// state instead of trusting the server to scope the stream.
type WSOrder struct {
	ID        string `json:"id"`
	AccountID string `json:"accountId"`
	Filled    string `json:"filled"`
	Status    string `json:"status"`
}

// Fill/terminal event types this client cares about — mirrors
// matching-engine/internal/models.EventType's order-lifecycle constants.
const (
	EventOrderFilled    = "ORDER_FILLED"
	EventOrderPartial   = "ORDER_PARTIALLY_FILLED"
	EventOrderCancelled = "ORDER_CANCELLED"
	EventOrderRejected  = "ORDER_REJECTED"
	EventOrderExpired   = "ORDER_EXPIRED"
)

// EventListener receives every decoded event this client's socket delivers.
// Called on the client's own read goroutine — must not block, and must not
// call back into WSClient (Subscribe/Unsubscribe are safe to call from other
// goroutines, but calling them synchronously from inside a listener risks a
// self-deadlock on the same mutex during the initial dispatch).
type EventListener func(evt WSEvent)

// ReconnectListener is invoked once per successful (re)connection, including
// the first one. Consumers use this to run a one-time REST reconciliation
// pass (e.g. GET /orders) so state that changed while disconnected — or
// during the gap between process start and the first successful connect —
// is not silently missed. This is the ONLY place polling belongs in this
// design: recovery after a known gap, not a steady-state timer.
type ReconnectListener func()

const (
	baseReconnectDelay = 1 * time.Second
	maxReconnectDelay  = 30 * time.Second
)

// WSClient maintains one persistent connection to the matching engine's
// broadcast /ws stream and fans out decoded events to subscribers. One
// instance is shared by the whole bots process (see runtime.Manager) — the
// hub broadcasts to every connected socket regardless of which accounts a
// given consumer cares about, so a single shared connection plus client-side
// filtering is strictly cheaper than one socket per desk.
type WSClient struct {
	url string
	// internalSecret, when non-empty, is sent as X-Engine-Ws-Secret on every
	// dial so the engine's CheckOrigin recognizes this as trusted
	// server-to-server traffic instead of evaluating it against the browser
	// Origin allowlist (which this process, having no Origin, would fail).
	internalSecret string

	mu          sync.Mutex
	listeners   map[int]EventListener
	reconnects  map[int]ReconnectListener
	nextID      int
	wantConn    bool
	conn        *websocket.Conn
	reconnectAt time.Duration
	closed      bool
}

// wsURLFromHTTP derives a ws(s):// URL for /ws from the engine's http(s)://
// base URL, e.g. "https://matching-engine-x.onrender.com" ->
// "wss://matching-engine-x.onrender.com/ws".
func wsURLFromHTTP(httpBaseURL string) (string, error) {
	u, err := url.Parse(httpBaseURL)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	default:
		u.Scheme = "ws"
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/ws"
	return u.String(), nil
}

// NewWSClient builds a client targeting the given engine HTTP base URL's /ws
// endpoint, authenticating as trusted server-to-server traffic with
// internalSecret (sent as X-Engine-Ws-Secret; must match the engine's
// WS_INTERNAL_SECRET). Call Start to begin connecting; it is a no-op until
// then. An empty internalSecret still builds a client, but every dial will be
// rejected by an engine that has WS_INTERNAL_SECRET or WS_ALLOWED_ORIGINS
// configured (this process has no Origin header to fall back on).
func NewWSClient(engineBaseURL string, internalSecret string) (*WSClient, error) {
	wsURL, err := wsURLFromHTTP(engineBaseURL)
	if err != nil {
		return nil, err
	}
	return &WSClient{
		url:            wsURL,
		internalSecret: internalSecret,
		listeners:      map[int]EventListener{},
		reconnects:     map[int]ReconnectListener{},
		reconnectAt:    baseReconnectDelay,
	}, nil
}

// Start begins the connect/reconnect loop in the background. Safe to call
// once at process startup; a second call is a no-op.
func (c *WSClient) Start() {
	c.mu.Lock()
	if c.wantConn {
		c.mu.Unlock()
		return
	}
	c.wantConn = true
	c.mu.Unlock()
	go c.run()
}

// Stop tears down the connection and stops reconnecting. Intended for
// graceful shutdown only.
func (c *WSClient) Stop() {
	c.mu.Lock()
	c.wantConn = false
	c.closed = true
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// OnEvent registers a listener for every event the stream delivers and
// returns an unsubscribe function. Consumers filter by Order.AccountID (or
// Symbol/Market) themselves.
func (c *WSClient) OnEvent(fn EventListener) (unsubscribe func()) {
	c.mu.Lock()
	id := c.nextID
	c.nextID++
	c.listeners[id] = fn
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		delete(c.listeners, id)
		c.mu.Unlock()
	}
}

// OnReconnect registers a listener invoked after each successful connect
// (including the first). Use it to trigger a one-time REST reconciliation —
// see ReconnectListener's doc comment.
func (c *WSClient) OnReconnect(fn ReconnectListener) (unsubscribe func()) {
	c.mu.Lock()
	id := c.nextID
	c.nextID++
	c.reconnects[id] = fn
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		delete(c.reconnects, id)
		c.mu.Unlock()
	}
}

func (c *WSClient) run() {
	for {
		c.mu.Lock()
		want := c.wantConn
		c.mu.Unlock()
		if !want {
			return
		}

		var hdr http.Header
		if c.internalSecret != "" {
			hdr = http.Header{"X-Engine-Ws-Secret": []string{c.internalSecret}}
		}
		conn, _, err := websocket.DefaultDialer.Dial(c.url, hdr)
		if err != nil {
			slog.Warn("engine ws: dial failed, retrying", "url", c.url, "error", err)
			if !c.sleepBackoff() {
				return
			}
			continue
		}

		c.mu.Lock()
		if !c.wantConn {
			c.mu.Unlock()
			_ = conn.Close()
			return
		}
		c.conn = conn
		c.reconnectAt = baseReconnectDelay // reset backoff on success
		c.mu.Unlock()

		slog.Info("engine ws: connected", "url", c.url)
		c.dispatchReconnect()
		c.readLoop(conn)

		c.mu.Lock()
		if c.conn == conn {
			c.conn = nil
		}
		wantAfter := c.wantConn
		c.mu.Unlock()
		if !wantAfter {
			return
		}
		if !c.sleepBackoff() {
			return
		}
	}
}

// sleepBackoff waits the current backoff (with jitter), doubling it for next
// time, capped at maxReconnectDelay. Returns false if Stop was called during
// the wait, meaning the caller should exit rather than reconnect.
func (c *WSClient) sleepBackoff() bool {
	c.mu.Lock()
	delay := c.reconnectAt
	c.reconnectAt = time.Duration(float64(c.reconnectAt) * 2)
	if c.reconnectAt > maxReconnectDelay {
		c.reconnectAt = maxReconnectDelay
	}
	c.mu.Unlock()

	jitter := time.Duration(rand.Int63n(int64(250 * time.Millisecond)))
	timer := time.NewTimer(delay + jitter)
	defer timer.Stop()
	<-timer.C

	c.mu.Lock()
	want := c.wantConn
	c.mu.Unlock()
	return want
}

func (c *WSClient) dispatchReconnect() {
	c.mu.Lock()
	fns := make([]ReconnectListener, 0, len(c.reconnects))
	for _, fn := range c.reconnects {
		fns = append(fns, fn)
	}
	c.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

func (c *WSClient) readLoop(conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			c.mu.Lock()
			closed := c.closed
			c.mu.Unlock()
			if !closed {
				slog.Warn("engine ws: read failed, reconnecting", "error", err)
			}
			return
		}
		var evt WSEvent
		if err := json.Unmarshal(data, &evt); err != nil {
			continue // malformed frame; skip
		}
		c.mu.Lock()
		fns := make([]EventListener, 0, len(c.listeners))
		for _, fn := range c.listeners {
			fns = append(fns, fn)
		}
		c.mu.Unlock()
		for _, fn := range fns {
			fn(evt)
		}
	}
}
