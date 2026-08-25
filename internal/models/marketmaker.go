package models

import "time"

// MarketMaker is one admin-managed market-making desk: a single (base, market)
// pair funded by the treasury. allocated_usdc is the admin-attested bookkeeping
// number — the source of truth for how much quote capital backs this desk. It
// mirrors real USDC the admin moved into the treasury wallet off-platform; the
// platform never detects on-chain movements, it trusts this number.
//
// bot_id links to the underlying bots row that actually quotes (strategy
// "market_maker"). The MM funding layer keeps allocated_usdc, the engine ledger
// balance of the MM wallet, and the bots.investment in sync ("one number, three
// places").
type MarketMaker struct {
	ID            string    `json:"id"`
	Base          string    `json:"base"`   // e.g. "BTC"
	Market        Market    `json:"market"` // SPOT | FUTURES
	Symbol        string    `json:"symbol"` // e.g. "BTC-USDC"
	WalletAddress string    `json:"walletAddress"`
	BotID         string    `json:"botId"`
	AllocatedUSDC string    `json:"allocatedUsdc"`
	Enabled       bool      `json:"enabled"` // admin start/stop flag
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

// MMFundingEntry is one immutable audit row in the funding ledger. Every
// deposit/withdraw the admin records writes one row so the allocated_usdc
// balance is always reconstructable and every treasury move is traceable.
type MMFundingEntry struct {
	ID           string    `json:"id"`
	MarketMakerID string   `json:"marketMakerId"`
	Direction    string    `json:"direction"` // "deposit" | "withdraw"
	Amount       string    `json:"amount"`
	BalanceAfter string    `json:"balanceAfter"`
	AdminID      string    `json:"adminId"`
	Note         string    `json:"note,omitempty"`
	CreatedAt    time.Time `json:"createdAt"`
}
