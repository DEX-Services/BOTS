// Package backend is the bots service's HTTP client for Dex-Backend's internal
// balance API. Dex-Backend's Postgres user_balances is the authoritative source
// of truth the matching engine risk-locks against; the engine's in-memory
// ledger is only a downstream mirror. The market-maker funding layer therefore
// credits/debits the MM wallet's real balance here so the engine's order
// balance lock succeeds.
package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// rawUnitScale is the fixed-point scale Dex-Backend stores balances in: a
// balance is an integer count of 10^-6 units, so a dollar amount is scaled by
// 10^6 to its raw integer form. Must match matching-engine's RawUnitScale.
const rawUnitScale = 6

// Client calls Dex-Backend's internal balance endpoints.
type Client struct {
	baseURL      string
	engineSecret string
	http         *http.Client
}

// NewClient builds a Dex-Backend client. secret authenticates internal calls
// via the X-Engine-Secret header (Dex-Backend's ENGINE_SHARED_SECRET).
func NewClient(baseURL, secret string) *Client {
	return &Client{
		baseURL:      strings.TrimRight(baseURL, "/"),
		engineSecret: secret,
		http:         &http.Client{Timeout: 10 * time.Second},
	}
}

// ResetBalance zeroes the MM desk wallet's free and locked balance for an asset
// via /internal/balance/reset, reclaiming capital and releasing any locks
// orphaned by an engine restart. Called on desk deletion.
func (c *Client) ResetBalance(ctx context.Context, userID, asset string) error {
	if c.engineSecret == "" {
		return fmt.Errorf("engine secret not configured; cannot reset backend balance")
	}
	body, err := json.Marshal(map[string]string{"userId": userID, "asset": asset})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/balance/reset", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", c.engineSecret)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("backend reset %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

// ReleaseLocks zeroes ONLY the MM desk wallet's locked balance for an asset via
// /internal/balance/release-locks, preserving its free capital. Called on bots
// startup (recredit) to clear holds orphaned by a matching-engine restart, so
// the desk can lock margin for new quotes again.
func (c *Client) ReleaseLocks(ctx context.Context, userID, asset string) error {
	if c.engineSecret == "" {
		return fmt.Errorf("engine secret not configured; cannot release backend locks")
	}
	body, err := json.Marshal(map[string]string{"userId": userID, "asset": asset})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/balance/release-locks", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", c.engineSecret)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("backend release-locks %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

// SyncBalance restores an internal desk wallet's durable balance to its
// authoritative allocation after a matching-engine restart. The amount is a
// human-unit decimal and is converted to the backend's fixed-point units.
func (c *Client) SyncBalance(ctx context.Context, userID, asset string, amount decimal.Decimal) error {
	if c.engineSecret == "" {
		return fmt.Errorf("engine secret not configured; cannot synchronize backend balance")
	}
	body, err := json.Marshal(map[string]string{
		"userId": userID, "asset": asset, "amount": toRawUnits(amount),
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/balance/sync", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", c.engineSecret)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("backend sync balance %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

// EnsureUser creates a synthetic Dex-Backend users row for the MM desk wallet
// via /internal/user/ensure, so its user_balances credits/locks satisfy the
// foreign key to users. Idempotent. Must be called before the first Deposit.
func (c *Client) EnsureUser(ctx context.Context, userID string) error {
	if c.engineSecret == "" {
		return fmt.Errorf("engine secret not configured; cannot ensure backend user")
	}
	body, err := json.Marshal(map[string]string{"userId": userID})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/user/ensure", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", c.engineSecret)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("backend ensure user %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

// CreditBalance adjusts the MM wallet's real Postgres balance via
// /internal/balance/credit. A positive amount credits; a negative amount
// debits. The dollar amount is scaled to raw integer units before sending.
func (c *Client) CreditBalance(ctx context.Context, userID, asset string, amount decimal.Decimal) error {
	if c.engineSecret == "" {
		return fmt.Errorf("engine secret not configured; cannot credit backend balance")
	}
	raw := toRawUnits(amount)
	body, err := json.Marshal(map[string]string{
		"userId": userID, "asset": asset, "amount": raw,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/balance/credit", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", c.engineSecret)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("backend credit %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

// toRawUnits scales a signed dollar amount to Dex-Backend's raw integer units
// (amount × 10^rawUnitScale), truncating any sub-unit remainder. Sign is
// preserved so a negative amount round-trips as a debit.
func toRawUnits(amount decimal.Decimal) string {
	scaled := amount.Shift(rawUnitScale).Truncate(0)
	i := new(big.Int)
	i.SetString(scaled.String(), 10)
	return i.String()
}
