package api

import "testing"

func TestValidateUserStrategy_BlocksMarketMakers(t *testing.T) {
	for _, k := range []string{"market_maker", "options_market_maker"} {
		if err := validateUserStrategy(k); err == nil {
			t.Errorf("validateUserStrategy(%q) = nil, want error", k)
		}
	}
	for _, k := range []string{"spot_grid", "futures_dca", "futures_twap"} {
		if err := validateUserStrategy(k); err != nil {
			t.Errorf("validateUserStrategy(%q) = %v, want nil", k, err)
		}
	}
}

func TestValidateUserInvestment_EnforcesFloor(t *testing.T) {
	for _, v := range []string{"0", "1", "9.99", "", "abc"} {
		if err := validateUserInvestment(v); err == nil {
			t.Errorf("validateUserInvestment(%q) = nil, want error", v)
		}
	}
	for _, v := range []string{"10", "10.01", "500"} {
		if err := validateUserInvestment(v); err != nil {
			t.Errorf("validateUserInvestment(%q) = %v, want nil", v, err)
		}
	}
}
