// Package engine is the bots service's HTTP client for the matching engine.
// Bots are ordinary clients of the exchange: they submit orders through the
// same public /order and /cancel endpoints real users hit, identified by the
// user's wallet address (the engine account ID). A shared http.Client with
// keep-alive plus a global concurrency semaphore bounds load on the engine.
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// Client calls the matching engine's HTTP API.
type Client struct {
	baseURL string
	http    *http.Client
	sem     chan struct{}
	// engineSecret authenticates privileged calls (/internal/ledger/sync).
	// Empty ⇒ LedgerSync returns an error rather than calling unauthenticated.
	// atomic.Value (not a plain string) because SetEngineSecret and every
	// LedgerSync call read/write this concurrently with no other
	// synchronization — a plain field here was a real data race (Low-1) if
	// SetEngineSecret were ever called while a request was in flight.
	engineSecret atomic.Value
}

// NewClient builds an engine client. concurrency bounds in-flight calls.
func NewClient(baseURL string, concurrency int) *Client {
	if concurrency <= 0 {
		concurrency = 256
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        512,
				MaxIdleConnsPerHost: 256,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		sem: make(chan struct{}, concurrency),
	}
}

// SetEngineSecret sets the shared secret used to authenticate privileged
// engine calls (/internal/ledger/sync).
func (c *Client) SetEngineSecret(secret string) { c.engineSecret.Store(secret) }

// getEngineSecret returns the current secret, or "" if never set.
func (c *Client) getEngineSecret() string {
	v, _ := c.engineSecret.Load().(string)
	return v
}

// LedgerSync credits or debits an account's in-memory engine ledger balance
// via /internal/ledger/sync. direction must be "credit" or "debit". Used by
// the market-maker funding layer to fund/defund MM wallets. Returns an error
// if the engine secret is unset or the engine rejects the change (e.g. a debit
// exceeding balance).
func (c *Client) LedgerSync(ctx context.Context, account, asset, amount, direction string) error {
	secret := c.getEngineSecret()
	if secret == "" {
		return fmt.Errorf("engine secret not configured; cannot sync ledger")
	}
	body, err := json.Marshal(map[string]string{
		"accountId": account, "asset": asset, "amount": amount, "direction": direction,
		// requestId (M4) lets the engine recognize a resend of this exact
		// call as a duplicate rather than re-applying it — see
		// matching-engine's ledgerSyncDedup. LedgerSync itself doesn't
		// retry internally, but a caller one level up (or a client-side
		// timeout that resends after the first attempt actually landed)
		// can still produce two physical requests for one logical call.
		"requestId": uuid.NewString(),
	})
	if err != nil {
		return err
	}
	if err := c.acquire(ctx); err != nil {
		return err
	}
	defer c.release()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/ledger/sync", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", secret)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("engine ledger sync %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

func (c *Client) acquire(ctx context.Context) error {
	select {
	case c.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) release() { <-c.sem }

// OrderResponse is the engine's /order reply.
type OrderResponse struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Filled  string `json:"filled"`
	Trades  int    `json:"trades"`
}

type MarketMakerQuote struct {
	Side  string `json:"side"`
	Price string `json:"price"`
	Qty   string `json:"qty"`
}

type MarketMakerReplaceResponse struct {
	Status string      `json:"status"`
	Orders []OpenOrder `json:"orders"`
	// Removed carries the final Filled/Status of every order this replace
	// cancelled from the previous ladder — see the engine's MMReplaceResponse
	// doc comment for why this matters (a fill in the instant before
	// cancellation is otherwise invisible to the strategy forever).
	Removed []OpenOrder `json:"removed"`
}

// OpenOrder is one entry from /orders.
type OpenOrder struct {
	ID     string `json:"id"`
	Symbol string `json:"symbol"`
	Market string `json:"market"`
	Side   string `json:"side"`
	Price  string `json:"price"`
	Qty    string `json:"qty"`
	Filled string `json:"filled"`
	Status string `json:"status"`
}

// FuturesPosition is one entry from /positions.
type FuturesPosition struct {
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	Size          string `json:"size"`
	EntryPrice    string `json:"entryPrice"`
	MarkPrice     string `json:"markPrice"`
	Margin        string `json:"margin"`
	Leverage      int    `json:"leverage"`
	UnrealizedPnl string `json:"unrealizedPnl"`
}

// Ticker is parsed from the engine's plain-text /ticker response.
type Ticker struct {
	Symbol string
	Market string
	Bid    decimal.Decimal
	Ask    decimal.Decimal
	Mid    decimal.Decimal
	Spread decimal.Decimal
	// Mark is the engine's markPrice — populated from the last trade even
	// when the book currently has no resting orders on one or both sides
	// (so Bid/Ask/Mid are all zero). Used as a last-resort price source so a
	// symbol that has traded, but whose book is momentarily empty, doesn't
	// look identical to a symbol that has never traded at all.
	Mark decimal.Decimal
}

// Balance is parsed from /admin/balance.
type Balance struct {
	Account   string
	Asset     string
	Balance   decimal.Decimal
	Reserved  decimal.Decimal
	Available decimal.Decimal
}

// OptionChainEntry is one listed contract's live quote/greeks from the
// engine's GET /option-chain, as populated by cmd/engine/main.go — theoretical
// Black-Scholes price/greeks, blended with the contract's own book mid once
// it has live two-sided quotes.
type OptionChainEntry struct {
	Symbol     string  `json:"symbol"`
	OptionType string  `json:"optionType"` // "CALL" | "PUT"
	Strike     string  `json:"strike"`
	Expiry     string  `json:"expiry"` // RFC3339
	Bid        string  `json:"bid"`
	Ask        string  `json:"ask"`
	Mid        string  `json:"mid"`
	IV         float64 `json:"iv"`
	Delta      float64 `json:"delta"`
	Gamma      float64 `json:"gamma"`
	Theta      float64 `json:"theta"`
	Vega       float64 `json:"vega"`
	Rho        float64 `json:"rho"`
}

// OptionChain fetches the live option chain for underlying (e.g. "BTC-BI2XUSD").
func (c *Client) OptionChain(ctx context.Context, underlying string) ([]OptionChainEntry, error) {
	var resp struct {
		Underlying string             `json:"underlying"`
		Spot       string             `json:"spot"`
		Chain      []OptionChainEntry `json:"chain"`
	}
	if err := c.get(ctx, "/option-chain?underlying="+url.QueryEscape(underlying), &resp); err != nil {
		return nil, err
	}
	return resp.Chain, nil
}

// SubmitOrder places an order on the engine. price is ignored for MARKET.
func (c *Client) SubmitOrder(ctx context.Context, account, symbol, market, side, orderType string, price, qty decimal.Decimal, leverage int, marginMode string) (OrderResponse, error) {
	q := url.Values{}
	q.Set("account", account)
	q.Set("symbol", symbol)
	q.Set("market", market)
	q.Set("side", side)
	q.Set("type", orderType)
	q.Set("price", price.String())
	q.Set("qty", qty.String())
	if leverage > 0 {
		q.Set("leverage", strconv.Itoa(leverage))
	}
	if marginMode != "" {
		q.Set("marginMode", marginMode)
	}
	var out OrderResponse
	if err := c.post(ctx, "/order?"+q.Encode(), nil, &out); err != nil {
		return OrderResponse{}, err
	}
	return out, nil
}

// ReplaceMarketMakerLadder atomically replaces an MM account's complete
// passive ladder. All orders become visible together on the engine side.
func (c *Client) ReplaceMarketMakerLadder(ctx context.Context, account, symbol, market string, reference decimal.Decimal, referenceTimestampMs int64, quotes []MarketMakerQuote) (MarketMakerReplaceResponse, error) {
	body, err := json.Marshal(map[string]any{
		"account": account, "symbol": symbol, "market": market,
		"referencePrice": reference.String(), "referenceTimestampMs": referenceTimestampMs,
		"orders": quotes,
	})
	if err != nil {
		return MarketMakerReplaceResponse{}, err
	}
	var out MarketMakerReplaceResponse
	if err := c.post(ctx, "/market-maker/replace", bytes.NewReader(body), &out); err != nil {
		return MarketMakerReplaceResponse{}, err
	}
	return out, nil
}

// ClearMarketMakerLadder removes all live quotes for the dedicated MM account
// in one engine command and releases its aggregate reservation. Returns the
// same response ReplaceMarketMakerLadder would, including Removed — a fill
// can still land on a resting order in the instant before this clears it.
func (c *Client) ClearMarketMakerLadder(ctx context.Context, account, symbol, market string) (MarketMakerReplaceResponse, error) {
	return c.ReplaceMarketMakerLadder(ctx, account, symbol, market, decimal.Zero, time.Now().UnixMilli(), nil)
}

// CancelOrder cancels a resting order.
func (c *Client) CancelOrder(ctx context.Context, account, symbol, market, orderID string) (OrderResponse, error) {
	q := url.Values{}
	q.Set("account", account)
	q.Set("symbol", symbol)
	q.Set("market", market)
	q.Set("order_id", orderID)
	var out OrderResponse
	if err := c.post(ctx, "/cancel?"+q.Encode(), nil, &out); err != nil {
		return OrderResponse{}, err
	}
	return out, nil
}

// OpenOrders returns the account's resting orders across all symbols.
func (c *Client) OpenOrders(ctx context.Context, account string) ([]OpenOrder, error) {
	var resp struct {
		Orders []OpenOrder `json:"orders"`
	}
	if err := c.get(ctx, "/orders?account="+url.QueryEscape(account), &resp); err != nil {
		return nil, err
	}
	return resp.Orders, nil
}

// OrderStatus is the engine's reply for GET /order/status: the real, current
// state of one order. Found is false when neither the live book nor the durable
// record knows the order (e.g. not yet flushed); Resting is true only while it
// is still on the book. Filled is the authoritative filled quantity — callers
// account against it instead of assuming a vanished order filled in full.
type OrderStatus struct {
	OrderID string `json:"orderId"`
	Found   bool   `json:"found"`
	Resting bool   `json:"resting"`
	Status  string `json:"status"`
	Filled  string `json:"filled"`
}

// OrderStatusByID fetches one order's authoritative state. symbol/market let the
// engine check the correct live book first before falling back to Postgres.
func (c *Client) OrderStatusByID(ctx context.Context, symbol, market, orderID string) (OrderStatus, error) {
	q := url.Values{}
	q.Set("id", orderID)
	q.Set("symbol", symbol)
	q.Set("market", market)
	var out OrderStatus
	if err := c.get(ctx, "/order/status?"+q.Encode(), &out); err != nil {
		return OrderStatus{}, err
	}
	return out, nil
}

// FuturesPositions returns the account's open futures positions.
func (c *Client) FuturesPositions(ctx context.Context, account string) ([]FuturesPosition, error) {
	var resp struct {
		Futures []FuturesPosition `json:"futures"`
	}
	if err := c.get(ctx, "/positions?account="+url.QueryEscape(account), &resp); err != nil {
		return nil, err
	}
	return resp.Futures, nil
}

// tickerResponse mirrors the engine's actual GET /ticker JSON shape (see
// matching-engine/cmd/engine/ticker.go's TickerResponse) — bestBid/bestAsk/
// midPrice as decimal strings, not the "key=value" text format this client
// used to (incorrectly) expect. That mismatch made every field silently fail
// to parse and return a zero Ticker forever, which every strategy relying on
// a live mid price (grid, DCA, TWAP, market-maker) depends on to trade.
type tickerResponse struct {
	Symbol    string `json:"symbol"`
	Market    string `json:"market"`
	BestBid   string `json:"bestBid"`
	BestAsk   string `json:"bestAsk"`
	MidPrice  string `json:"midPrice"`
	Spread    string `json:"spread"`
	MarkPrice string `json:"markPrice"`
}

// Ticker fetches best bid/ask/mid for a symbol/market.
func (c *Client) Ticker(ctx context.Context, symbol, market string) (Ticker, error) {
	var resp tickerResponse
	if err := c.get(ctx, "/ticker?symbol="+url.QueryEscape(symbol)+"&market="+url.QueryEscape(market), &resp); err != nil {
		return Ticker{}, err
	}
	return parseTicker(resp)
}

// Balance fetches the in-memory ledger balance for an account/asset.
func (c *Client) Balance(ctx context.Context, account, asset string) (Balance, error) {
	var b Balance
	if err := c.get(ctx, "/admin/balance?account="+url.QueryEscape(account)+"&asset="+url.QueryEscape(asset), &b); err != nil {
		return Balance{}, err
	}
	return b, nil
}

func (c *Client) post(ctx context.Context, path string, body io.Reader, out any) error {
	if err := c.acquire(ctx); err != nil {
		return err
	}
	defer c.release()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, body)
	if err != nil {
		return err
	}
	return c.do(req, out)
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	if err := c.acquire(ctx); err != nil {
		return err
	}
	defer c.release()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	return c.do(req, out)
}

func (c *Client) do(req *http.Request, out any) error {
	if secret := c.getEngineSecret(); secret != "" {
		req.Header.Set("X-Engine-Secret", secret)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("engine %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(b, out)
}

// parseTicker converts the engine's JSON ticker fields (decimal strings) into
// a Ticker. A malformed numeric field is a hard error: silently coercing an
// unparseable price to zero would feed bots a fake market (bid/ask of 0), so
// surface it instead.
func parseTicker(r tickerResponse) (Ticker, error) {
	t := Ticker{Symbol: r.Symbol, Market: r.Market}
	var err error
	if r.BestBid != "" {
		if t.Bid, err = decimal.NewFromString(r.BestBid); err != nil {
			return Ticker{}, fmt.Errorf("ticker field %q=%q: %w", "bestBid", r.BestBid, err)
		}
	}
	if r.BestAsk != "" {
		if t.Ask, err = decimal.NewFromString(r.BestAsk); err != nil {
			return Ticker{}, fmt.Errorf("ticker field %q=%q: %w", "bestAsk", r.BestAsk, err)
		}
	}
	if r.MidPrice != "" {
		if t.Mid, err = decimal.NewFromString(r.MidPrice); err != nil {
			return Ticker{}, fmt.Errorf("ticker field %q=%q: %w", "midPrice", r.MidPrice, err)
		}
	}
	if r.Spread != "" {
		if t.Spread, err = decimal.NewFromString(r.Spread); err != nil {
			return Ticker{}, fmt.Errorf("ticker field %q=%q: %w", "spread", r.Spread, err)
		}
	}
	if r.MarkPrice != "" {
		if t.Mark, err = decimal.NewFromString(r.MarkPrice); err != nil {
			return Ticker{}, fmt.Errorf("ticker field %q=%q: %w", "markPrice", r.MarkPrice, err)
		}
	}
	if t.Mid.IsZero() && !t.Bid.IsZero() && !t.Ask.IsZero() {
		t.Mid = t.Bid.Add(t.Ask).Div(decimal.NewFromInt(2))
	}
	return t, nil
}
