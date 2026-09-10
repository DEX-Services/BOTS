package strategy

import (
	"testing"

	"github.com/shopspring/decimal"
)

// TestQuoteCost is a regression test for a real gap: optionsMarketMaker's
// `investment` field — whose config label reads "Total quote budget backing
// all quoted contracts" — was validated in the constructor, stored on the
// struct, and then never read again. The desk quoted the ENTIRE option chain,
// both sides, at full qtyPerContract with no cap at all: a 20-contract chain
// meant 40 live orders whose combined collateral could run well past what the
// desk was actually funded with. The only thing stopping it was the engine
// rejecting orders once the wallet ran dry — the budget was enforced by
// running out of money at an arbitrary point in the chain, not by the desk.
//
// quoteCost is the pure budget rule that now gates each quote.
func TestQuoteCost(t *testing.T) {
	qty := decimal.NewFromFloat(0.1)

	t.Run("buy pays the premium", func(t *testing.T) {
		got := quoteCost("BUY", decimal.NewFromInt(500), "60000", qty)
		want := decimal.NewFromInt(50) // 500 * 0.1
		if !got.Equal(want) {
			t.Fatalf("BUY cost = %s, want %s", got, want)
		}
	})

	t.Run("sell is collateralized against the strike, not the premium", func(t *testing.T) {
		got := quoteCost("SELL", decimal.NewFromInt(500), "60000", qty)
		want := decimal.NewFromInt(6000) // 60000 * 0.1, NOT 500 * 0.1
		if !got.Equal(want) {
			t.Fatalf("SELL cost = %s, want %s (writing must charge strike, not premium)", got, want)
		}
	})

	t.Run("sell costs far more than buy for the same contract", func(t *testing.T) {
		// The whole point of the strike-based rule: charging the premium on a
		// written option would under-count the desk's real commitment by orders
		// of magnitude, which is exactly how an uncapped desk over-commits.
		buy := quoteCost("BUY", decimal.NewFromInt(500), "60000", qty)
		sell := quoteCost("SELL", decimal.NewFromInt(500), "60000", qty)
		if !sell.GreaterThan(buy) {
			t.Fatalf("SELL cost %s must exceed BUY cost %s", sell, buy)
		}
	})

	t.Run("malformed strike falls back to premium, never free", func(t *testing.T) {
		for _, bad := range []string{"", "not-a-number", "0", "-5"} {
			got := quoteCost("SELL", decimal.NewFromInt(500), bad, qty)
			if !got.IsPositive() {
				t.Fatalf("strike %q produced non-positive cost %s; a bad chain entry must never be treated as free", bad, got)
			}
			if !got.Equal(decimal.NewFromInt(50)) {
				t.Fatalf("strike %q: cost = %s, want premium fallback 50", bad, got)
			}
		}
	})
}

// TestQuoteCost_BudgetExhaustionOrder verifies the accumulation behavior
// OnTick relies on: walking a chain and charging each side, the budget runs
// out partway rather than every quote being placed unconditionally.
func TestQuoteCost_BudgetExhaustionOrder(t *testing.T) {
	qty := decimal.NewFromInt(1)
	budget := decimal.NewFromInt(10000)
	spent := decimal.Zero
	placed := 0

	// Five contracts at a 6000 strike: writing costs 6000 each, so only the
	// first fits; the rest must be skipped rather than placed.
	for i := 0; i < 5; i++ {
		cost := quoteCost("SELL", decimal.NewFromInt(500), "6000", qty)
		if spent.Add(cost).LessThanOrEqual(budget) {
			spent = spent.Add(cost)
			placed++
		}
	}
	if placed != 1 {
		t.Fatalf("placed %d SELL quotes on a 10000 budget at 6000 each, want 1", placed)
	}
	if spent.GreaterThan(budget) {
		t.Fatalf("spent %s exceeded budget %s", spent, budget)
	}
}
