// Package models defines the domain types for the bots service: bot
// instances, strategy templates, runtime state, and performance stats.
package models

import (
	"time"

	"github.com/shopspring/decimal"
)

// Market mirrors the matching-engine market type (uppercase, matching the
// engine's /order "market" query parameter exactly so no conversion is needed).
type Market string

const (
	Spot    Market = "SPOT"
	Futures Market = "FUTURES"
)

// Status is the lifecycle state of a bot instance.
type Status string

const (
	StatusDraft   Status = "draft"   // created but never started
	StatusRunning Status = "running" // actively placing orders
	StatusPaused  Status = "paused"  // user-paused (orders left resting)
	StatusStopped Status = "stopped" // stopped; open orders cancelled
	StatusError   Status = "error"   // halted due to a failure
)

// Bot is a user-owned bot instance persisted in Postgres.
type Bot struct {
	ID            string                 `json:"id"`
	UserID        string                 `json:"userId"`
	WalletAddress string                 `json:"walletAddress"`
	Name          string                 `json:"name"`
	Strategy      string                 `json:"strategy"`
	Market        Market                 `json:"market"`
	Symbol        string                 `json:"symbol"`
	Investment    string                 `json:"investment"`
	Config        map[string]string      `json:"config"`
	IsPublic      bool                   `json:"isPublic"`
	Status        Status                 `json:"status"`
	IsRunning     bool                   `json:"isRunning"`
	State         map[string]any         `json:"state,omitempty"`
	Stats         Stats                  `json:"stats"`
	Error         string                 `json:"error,omitempty"`
	CreatedAt     time.Time              `json:"createdAt"`
	UpdatedAt     time.Time              `json:"updatedAt"`
	StartedAt     *time.Time             `json:"startedAt,omitempty"`
	StoppedAt     *time.Time             `json:"stoppedAt,omitempty"`
}

// Stats are the live performance metrics shown in the UI/marketplace.
type Stats struct {
	RealizedPnL    string `json:"realizedPnl"`
	UnrealizedPnL  string `json:"unrealizedPnl"`
	NetPnL         string `json:"netPnl"`
	ROI           string `json:"roi"`
	RuntimeSec     int64  `json:"runtimeSec"`
	MatchedTrades  int    `json:"matchedTrades"`
	Trades24h      int    `json:"trades24h"`
	MaxDrawdownPct string `json:"maxDrawdownPct"`
	BaseHeld       string `json:"baseHeld"`
	AvgEntryPrice  string `json:"avgEntryPrice"`
}

// NewStats returns a zero-valued Stats with decimal fields as "0".
func NewStats() Stats {
	return Stats{
		RealizedPnL:    "0",
		UnrealizedPnL:  "0",
		NetPnL:        "0",
		ROI:           "0",
		MaxDrawdownPct: "0",
		BaseHeld:       "0",
		AvgEntryPrice:  "0",
	}
}

// Template describes a create-able bot strategy for the UI.
type Template struct {
	Key        string            `json:"key"`
	Title      string            `json:"title"`
	Desc       string            `json:"desc"`
	Category   string            `json:"category"` // "Spot" | "Futures"
	Available  bool              `json:"available"`
	Params     []TemplateParam   `json:"params"`
}

// TemplateParam is one configurable field in the create-bot modal.
type TemplateParam struct {
	Key        string `json:"key"`
	Label      string `json:"label"`
	Type       string `json:"type"` // "text" | "number" | "select" | "interval"
	Required   bool   `json:"required"`
	Default    string `json:"default"`
	Help       string `json:"help,omitempty"`
	Options    []string `json:"options,omitempty"`
}

// CreateBotRequest is the body of POST /bots.
type CreateBotRequest struct {
	Name       string            `json:"name"`
	Strategy   string            `json:"strategy"`
	Market     Market            `json:"market"`
	Symbol     string            `json:"symbol"`
	Investment string            `json:"investment"`
	Config     map[string]string `json:"config"`
	IsPublic   bool              `json:"isPublic"`
}

// EquitySample is a timestamped equity point used for drawdown calc.
type EquitySample struct {
	T  time.Time       `json:"-"`
	Val decimal.Decimal `json:"-"`
}
