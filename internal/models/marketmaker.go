package models

import "time"

// MarketMaker is one admin-managed market-making desk: a single (base, market)
// pair funded by the treasury. BaseAmount and QuoteAmount are the admin-
// attested bookkeeping numbers — each leg is funded independently and
// directly (e.g. an actual BTC amount and an actual USDT amount for a
// BTC-USDT desk; an actual ETH amount and USDT amount for ETH-USDT), never
// derived from one another by a formula. They mirror real assets the admin
// moved into the treasury wallet off-platform; the platform never detects
// on-chain movements, it trusts these numbers.
//
// bot_id links to the underlying bots row that actually quotes (strategy
// "market_maker"). The MM funding layer keeps BaseAmount/QuoteAmount, the
// engine ledger balances of the MM wallet, and the strategy's own tracked
// inventory in sync.
type MarketMaker struct {
	ID            string    `json:"id"`
	Base          string    `json:"base"`   // e.g. "BTC"
	Market        Market    `json:"market"` // SPOT | FUTURES
	Symbol        string    `json:"symbol"` // e.g. "BTC-USDC"
	WalletAddress string    `json:"walletAddress"`
	BotID         string    `json:"botId"`
	BaseAmount    string    `json:"baseAmount"`  // e.g. actual BTC funded
	QuoteAmount   string    `json:"quoteAmount"` // e.g. actual USDT/USDC funded
	Enabled       bool      `json:"enabled"`     // admin start/stop flag
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

// MMFundingEntry is one immutable audit row in the funding ledger. Every
// deposit/withdraw the admin records writes one row (tagged with which leg,
// base or quote, it applied to) so each balance is always reconstructable and
// every treasury move is traceable.
type MMFundingEntry struct {
	ID            string    `json:"id"`
	MarketMakerID string    `json:"marketMakerId"`
	Asset         string    `json:"asset"`     // "base" | "quote"
	Direction     string    `json:"direction"` // "deposit" | "withdraw"
	Amount        string    `json:"amount"`
	BalanceAfter  string    `json:"balanceAfter"`
	AdminID       string    `json:"adminId"`
	Note          string    `json:"note,omitempty"`
	CreatedAt     time.Time `json:"createdAt"`
}
