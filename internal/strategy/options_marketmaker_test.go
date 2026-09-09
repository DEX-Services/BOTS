package strategy

import (
	"testing"

	"github.com/dex/bots/internal/models"
	"github.com/shopspring/decimal"
)

func optionsMMBot(config map[string]string) *models.Bot {
	merged := map[string]string{
		"investment": "50000", "spreadBps": "300", "qtyPerContract": "0.1",
	}
	for k, v := range config {
		merged[k] = v
	}
	return &models.Bot{Symbol: "BTC-BIUSD", Market: models.Options, Investment: "50000", Config: merged}
}

func TestNewOptionsMarketMaker_ValidConfig(t *testing.T) {
	s, err := newOptionsMarketMaker(optionsMMBot(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s == nil {
		t.Fatal("expected a non-nil strategy")
	}
}

func TestNewOptionsMarketMaker_RejectsTightSpread(t *testing.T) {
	// Below the 100bps floor documented in newOptionsMarketMaker.
	_, err := newOptionsMarketMaker(optionsMMBot(map[string]string{"spreadBps": "10"}))
	if err == nil {
		t.Fatal("expected an error for a spread below the options minimum")
	}
}

func TestNewOptionsMarketMaker_RejectsNonPositiveInvestment(t *testing.T) {
	_, err := newOptionsMarketMaker(optionsMMBot(map[string]string{"investment": "0"}))
	if err == nil {
		t.Fatal("expected an error for zero investment")
	}
}

func TestNewOptionsMarketMaker_RejectsNonPositiveQty(t *testing.T) {
	_, err := newOptionsMarketMaker(optionsMMBot(map[string]string{"qtyPerContract": "-1"}))
	if err == nil {
		t.Fatal("expected an error for non-positive qtyPerContract")
	}
}

func TestOptionsMM_ApplyFillIfAny_SellCreditsPremium(t *testing.T) {
	m := &optionsMarketMaker{state: newStatePtr()}
	ref := OrderRef{OrderID: "o1", Side: "SELL", Price: "500", Qty: "1", Kind: "BTC-BIUSD-60000-20260101-CALL"}
	m.applyFillIfAny(&ref, decimal.NewFromInt(1)) // full fill of 1 contract at 500
	if got := dec(m.state.RealizedPnL); !got.Equal(decimal.NewFromInt(500)) {
		t.Fatalf("realized pnl after a SELL fill = %s, want 500 (premium received)", got)
	}
	if m.state.MatchedTrades != 1 {
		t.Fatalf("matched trades = %d, want 1", m.state.MatchedTrades)
	}
}

func TestOptionsMM_ApplyFillIfAny_BuyDebitsPremium(t *testing.T) {
	m := &optionsMarketMaker{state: newStatePtr()}
	ref := OrderRef{OrderID: "o2", Side: "BUY", Price: "500", Qty: "1", Kind: "BTC-BIUSD-60000-20260101-CALL"}
	m.applyFillIfAny(&ref, decimal.NewFromInt(1))
	if got := dec(m.state.RealizedPnL); !got.Equal(decimal.NewFromInt(-500)) {
		t.Fatalf("realized pnl after a BUY fill = %s, want -500 (premium paid)", got)
	}
}

func TestOptionsMM_ApplyFillIfAny_PartialFillOnlyAppliesDelta(t *testing.T) {
	m := &optionsMarketMaker{state: newStatePtr()}
	ref := OrderRef{OrderID: "o3", Side: "SELL", Price: "200", Qty: "1", Kind: "BTC-BIUSD-60000-20260101-PUT", AppliedFilled: "0.3"}
	m.applyFillIfAny(&ref, decimal.NewFromFloat(0.5)) // delta = 0.2
	want := decimal.NewFromFloat(0.2).Mul(decimal.NewFromInt(200))
	if got := dec(m.state.RealizedPnL); !got.Equal(want) {
		t.Fatalf("realized pnl after partial fill delta = %s, want %s", got, want)
	}
}

func TestOptionsMM_ApplyFillIfAny_NoNewFillIsNoop(t *testing.T) {
	m := &optionsMarketMaker{state: newStatePtr()}
	ref := OrderRef{OrderID: "o4", Side: "SELL", Price: "200", Qty: "1", AppliedFilled: "1"}
	m.applyFillIfAny(&ref, decimal.NewFromInt(1)) // already fully applied
	if got := dec(m.state.RealizedPnL); !got.IsZero() {
		t.Fatalf("realized pnl with no new fill delta = %s, want 0", got)
	}
	if m.state.MatchedTrades != 0 {
		t.Fatalf("matched trades = %d, want 0 for a no-op fill", m.state.MatchedTrades)
	}
}
