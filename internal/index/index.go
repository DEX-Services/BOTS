// Package index reads the shared INDEX price that the Price-Fetcher service
// publishes to Redis (key "<prefix>:<BASE>", e.g. "price:BTC"). Market-maker
// bots quote around this external reference instead of the engine's own order
// book mid — quoting off the book would be circular, since the MM's own orders
// ARE the book.
//
// The reader is freshness-aware: every read carries the age of the price, and
// callers MUST refuse to quote when a price is stale or absent (a dead fetcher
// or a network gap would otherwise leave a bot quoting on a frozen number).
package index

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

// payload mirrors Price-Fetcher's price.IndexPrice JSON contract. The MM only
// quotes off Last, but the extra 24h stats are decoded too so the public
// /index endpoint can serve the frontend header the same numbers it used to
// pull from Binance directly.
type payload struct {
	Last          float64 `json:"last"`
	ChangePercent float64 `json:"change_percent"`
	High24h       float64 `json:"high_24h"`
	Low24h        float64 `json:"low_24h"`
	QuoteVolume   float64 `json:"quote_volume"`
	TimestampMs   int64   `json:"timestamp_ms"`
}

// Snapshot is a freshness-checked view of one asset's index price. Price/Fresh/
// AgeMs are what quoting and P/L rely on; the 24h stats are display-only and
// carry the source's last values regardless of freshness.
type Snapshot struct {
	Price       decimal.Decimal
	Fresh       bool  // false when the key is missing or older than maxAge
	AgeMs       int64 // age of the price in milliseconds (0 when unknown)
	TimestampMs int64 // source publication time; used to deduplicate MM refreshes

	ChangePercent float64 // 24h change %
	High24h       float64 // 24h high
	Low24h        float64 // 24h low
	QuoteVolume   float64 // 24h quote-asset volume (~USD)
}

// Reader fetches index prices from Redis.
type Reader struct {
	rdb    *redis.Client
	prefix string
	maxAge time.Duration
}

// New connects to Redis using uri (rediss:// for TLS). prefix namespaces the
// keys and MUST match Price-Fetcher's PRICE_KEY_PREFIX. maxAge bounds how old
// a price may be before it is treated as stale.
func New(ctx context.Context, uri, prefix string, maxAge time.Duration) (*Reader, error) {
	if uri == "" {
		return nil, fmt.Errorf("REDIS_SERVICE_URI is not set")
	}
	opts, err := redis.ParseURL(uri)
	if err != nil {
		return nil, fmt.Errorf("parse redis URI: %w", err)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	if maxAge <= 0 {
		maxAge = 5 * time.Second
	}
	return &Reader{rdb: rdb, prefix: prefix, maxAge: maxAge}, nil
}

// Get returns the current index snapshot for a base asset (e.g. "BTC"). A
// missing key, malformed value, non-positive price, or a price older than
// maxAge all yield Fresh=false.
//
// nowMs is the caller's current time in Unix milliseconds. It is passed in
// rather than read here so the reader stays deterministic and testable.
func (r *Reader) Get(ctx context.Context, asset string, nowMs int64) Snapshot {
	return r.get(ctx, r.key(asset), nowMs)
}

// GetExact is like Get, but looks up "<prefix>:<ticker>" with the ticker's
// casing preserved exactly as given — no upper-casing. Price-Fetcher writes
// most tickers upper-case (BTC, EURUSD, GOLD) but Live-Rates.com stock
// tickers are written with a case-sensitive suffix (AAPL.us, not AAPL.US —
// see Price-Fetcher/README.md); Get's blanket ToUpper would silently miss
// those keys. Callers that already have the exact ticker (e.g. the
// /index/{base} HTTP handler, which takes it straight from the caller) use
// this instead of Get, which is for parsed-out crypto pair bases.
func (r *Reader) GetExact(ctx context.Context, ticker string, nowMs int64) Snapshot {
	return r.get(ctx, fmt.Sprintf("%s:%s", r.prefix, ticker), nowMs)
}

func (r *Reader) get(ctx context.Context, key string, nowMs int64) Snapshot {
	raw, err := r.rdb.Get(ctx, key).Bytes()
	if err != nil {
		// redis.Nil (key absent / expired via TTL) or any transport error.
		return Snapshot{Fresh: false}
	}
	var p payload
	if err := json.Unmarshal(raw, &p); err != nil {
		return Snapshot{Fresh: false}
	}
	price := decimal.NewFromFloat(p.Last)
	if !price.IsPositive() {
		return Snapshot{Fresh: false}
	}
	age := nowMs - p.TimestampMs
	if age < 0 {
		age = 0
	}
	fresh := age <= r.maxAge.Milliseconds()
	return Snapshot{
		Price: price, Fresh: fresh, AgeMs: age, TimestampMs: p.TimestampMs,
		ChangePercent: p.ChangePercent, High24h: p.High24h,
		Low24h: p.Low24h, QuoteVolume: p.QuoteVolume,
	}
}

// Close releases the Redis connection.
func (r *Reader) Close() error { return r.rdb.Close() }

// key builds "<prefix>:<BASE>" (upper-cased base), matching Price-Fetcher's
// crypto/forex/commodity keys. NOT used for stock tickers — see GetExact.
func (r *Reader) key(asset string) string {
	return fmt.Sprintf("%s:%s", r.prefix, strings.ToUpper(asset))
}
