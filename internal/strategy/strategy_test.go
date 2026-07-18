package strategy

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

// TestApplyFillsBuyThenSell checks avg-cost net-inventory accounting: a buy
// then a sell at a higher price realizes the expected profit and zeroes the
// position.
func TestApplyFillsBuyThenSell(t *testing.T) {
	s := newState()
	s.applyBuyFill(decimal.NewFromInt(2), decimal.NewFromInt(100)) // buy 2 @ 100
	s.applySellFill(decimal.NewFromInt(2), decimal.NewFromInt(120)) // sell 2 @ 120

	if got := dec(s.RealizedPnL); !got.Equal(decimal.NewFromInt(40)) {
		t.Fatalf("realizedPnL = %s, want 40", got)
	}
	if got := dec(s.BaseHeld); !got.IsZero() {
		t.Fatalf("baseHeld = %s, want 0", got)
	}
	if got := dec(s.QuoteCost); !got.IsZero() {
		t.Fatalf("quoteCost = %s, want 0", got)
	}
	if got := dec(s.AvgEntry); !got.IsZero() {
		t.Fatalf("avgEntry = %s, want 0 after flat", got)
	}
}

// TestMaxDrawdown verifies peak-to-trough drawdown over equity samples.
func TestMaxDrawdown(t *testing.T) {
	s := newState()
	now := time.Unix(0, 0)
	pts := []int64{100, 120, 90, 110, 60}
	for i, v := range pts {
		s.pushEquity(now.Add(time.Duration(i)*time.Hour), decimal.NewFromInt(v))
	}
	// peak 120, trough 60 -> dd = 50%
	got := s.MaxDrawdown()
	want := decimal.NewFromInt(50)
	if !got.Equal(want) {
		t.Fatalf("maxDrawdown = %s, want 50", got)
	}
}

// TestTrades24h verifies the rolling 24h window pruning.
func TestTrades24h(t *testing.T) {
	s := newState()
	now := time.Unix(1_700_000_000, 0)
	for i := range 5 {
		s.recordTrade(now.Add(-time.Duration(i) * time.Hour))
	}
	// 0..4h ago = 5 trades within 24h
	if got := s.Trades24h(now); got != 5 {
		t.Fatalf("trades24h = %d, want 5", got)
	}
	// 30h ago is outside the window
	s.recordTrade(now.Add(-30 * time.Hour))
	if got := s.Trades24h(now); got != 5 {
		t.Fatalf("trades24h = %d, want 5 (30h sample excluded)", got)
	}
	if s.MatchedTrades != 6 {
		t.Fatalf("matchedTrades = %d, want 6", s.MatchedTrades)
	}
}
