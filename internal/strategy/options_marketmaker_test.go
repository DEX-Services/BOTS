package strategy

import (
	"testing"

	"github.com/dex/bots/internal/engine"
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

func TestOptionsMM_ApplyFillIfAny_TracksSignedPosition(t *testing.T) {
	m := &optionsMarketMaker{state: newStatePtr()}
	symbol := "BTC-BIUSD-60000-20260101-CALL"

	buy := OrderRef{OrderID: "o5", Side: "BUY", Price: "500", Kind: symbol}
	m.applyFillIfAny(&buy, decimal.NewFromFloat(0.5))
	if got := dec(m.state.ContractPositions[symbol]); !got.Equal(decimal.NewFromFloat(0.5)) {
		t.Fatalf("position after a 0.5 BUY fill = %s, want 0.5", got)
	}

	sell := OrderRef{OrderID: "o6", Side: "SELL", Price: "510", Kind: symbol}
	m.applyFillIfAny(&sell, decimal.NewFromFloat(0.2))
	if got := dec(m.state.ContractPositions[symbol]); !got.Equal(decimal.NewFromFloat(0.3)) {
		t.Fatalf("position after a further 0.2 SELL fill = %s, want 0.3", got)
	}

	// Selling the remainder should zero out and remove the map entry
	// entirely (not leave a "0" string sitting around).
	sellRest := OrderRef{OrderID: "o7", Side: "SELL", Price: "505", Kind: symbol}
	m.applyFillIfAny(&sellRest, decimal.NewFromFloat(0.3))
	if _, ok := m.state.ContractPositions[symbol]; ok {
		t.Fatalf("expected ContractPositions to drop the entry once flat, still has %s", m.state.ContractPositions[symbol])
	}
}

func TestOptionsMM_NetDelta_ZeroWithNoPositions(t *testing.T) {
	m := &optionsMarketMaker{state: newStatePtr()}
	chain := []engine.OptionChainEntry{{Symbol: "BTC-BIUSD-60000-20260101-CALL", Delta: 0.5}}
	if got := m.netDelta(chain); !got.IsZero() {
		t.Fatalf("netDelta with no held positions = %s, want 0", got)
	}
}

func TestOptionsMM_NetDelta_WeightsHeldPositionsByChainDelta(t *testing.T) {
	m := &optionsMarketMaker{state: newStatePtr()}
	callSymbol := "BTC-BIUSD-60000-20260101-CALL"
	putSymbol := "BTC-BIUSD-50000-20260101-PUT"
	m.state.ContractPositions = map[string]string{
		callSymbol: "2",  // long 2 calls, delta 0.6 each -> +1.2
		putSymbol:  "-3", // short 3 puts, delta -0.4 each -> (-3)*(-0.4) = +1.2
	}
	chain := []engine.OptionChainEntry{
		{Symbol: callSymbol, Delta: 0.6},
		{Symbol: putSymbol, Delta: -0.4},
	}
	got := m.netDelta(chain)
	want := decimal.NewFromFloat(2.4)
	if !got.Equal(want) {
		t.Fatalf("netDelta = %s, want %s", got, want)
	}
}

func TestOptionsMM_NetDelta_IgnoresPositionsNotInChain(t *testing.T) {
	m := &optionsMarketMaker{state: newStatePtr()}
	expiredSymbol := "BTC-BIUSD-40000-20250101-CALL"
	m.state.ContractPositions = map[string]string{expiredSymbol: "5"}
	// Contract has expired and fallen off the live chain — its delta is
	// unknowable, so it must not contribute (and must not panic on a missing
	// map lookup).
	got := m.netDelta(nil)
	if !got.IsZero() {
		t.Fatalf("netDelta for a position not in the chain = %s, want 0", got)
	}
}
