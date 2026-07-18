// Package engine is the bots service's HTTP client for the matching engine.
// Bots are ordinary clients of the exchange: they submit orders through the
// same public /order and /cancel endpoints real users hit, identified by the
// user's wallet address (the engine account ID). A shared http.Client with
// keep-alive plus a global concurrency semaphore bounds load on the engine.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// Client calls the matching engine's HTTP API.
type Client struct {
	baseURL string
	http    *http.Client
	sem     chan struct{}
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
				IdleConnTimeout:    90 * time.Second,
			},
		},
		sem: make(chan struct{}, concurrency),
	}
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
}

// Balance is parsed from /admin/balance.
type Balance struct {
	Account   string
	Asset     string
	Balance   decimal.Decimal
	Reserved  decimal.Decimal
	Available decimal.Decimal
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

// CancelOrder cancels a resting order.
func (c *Client) CancelOrder(ctx context.Context, symbol, market, orderID string) (OrderResponse, error) {
	q := url.Values{}
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

// Ticker fetches best bid/ask/mid for a symbol/market.
func (c *Client) Ticker(ctx context.Context, symbol, market string) (Ticker, error) {
	body, err := c.getRaw(ctx, "/ticker?symbol="+url.QueryEscape(symbol)+"&market="+url.QueryEscape(market))
	if err != nil {
		return Ticker{}, err
	}
	return parseTicker(body), nil
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

func (c *Client) getRaw(ctx context.Context, path string) (string, error) {
	if err := c.acquire(ctx); err != nil {
		return "", err
	}
	defer c.release()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("engine %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return string(b), nil
}

func (c *Client) do(req *http.Request, out any) error {
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

// parseTicker parses "symbol=BTC-USDT market=spot bid=.. ask=.. mid=.. spread=..".
func parseTicker(s string) Ticker {
	t := Ticker{}
	for _, field := range strings.Fields(strings.TrimSpace(s)) {
		k, v, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch k {
		case "symbol":
			t.Symbol = v
		case "market":
			t.Market = v
		case "bid":
			t.Bid, _ = decimal.NewFromString(v)
		case "ask":
			t.Ask, _ = decimal.NewFromString(v)
		case "mid":
			t.Mid, _ = decimal.NewFromString(v)
		case "spread":
			t.Spread, _ = decimal.NewFromString(v)
		}
	}
	if t.Mid.IsZero() && !t.Bid.IsZero() && !t.Ask.IsZero() {
		t.Mid = t.Bid.Add(t.Ask).Div(decimal.NewFromInt(2))
	}
	return t
}
