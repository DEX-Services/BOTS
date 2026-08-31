// mm.go holds the admin market-maker desk handlers: list/create/get/deposit/
// withdraw/enable/history. All are gated by requireAdmin in Routes.
package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/dex/bots/internal/auth"
	"github.com/dex/bots/internal/models"
	"github.com/dex/bots/internal/store"
	"github.com/shopspring/decimal"
)

// adminID returns the authenticated admin's user id for audit logging.
func adminID(r *http.Request) string {
	if c := auth.FromRequest(r); c != nil {
		return c.UserID
	}
	return ""
}

// mmDeskView is a desk plus its live bot metrics and the current index price,
// so the admin dashboard can render funding + realtime P/L in one payload.
type mmDeskView struct {
	models.MarketMaker
	IsRunning    bool              `json:"isRunning"`
	IndexPrice   string            `json:"indexPrice"`
	IndexFresh   bool              `json:"indexFresh"`
	Config       map[string]string `json:"config"`
	Stats        models.Stats      `json:"stats"`
	QuoteAsset   string            `json:"quoteAsset"`
	QuoteBalance string            `json:"quoteBalance"`
	BaseBalance  string            `json:"baseBalance"`
}

func (s *Server) deskView(r *http.Request, desk *models.MarketMaker) mmDeskView {
	v := mmDeskView{MarketMaker: *desk, IsRunning: s.manager.IsRunning(desk.BotID), Stats: models.NewStats()}
	if bot, err := s.store.Get(r.Context(), desk.BotID); err == nil {
		v.Stats = bot.Stats
		v.Config = bot.Config
	}
	idx := s.manager.IndexSnapshot(r.Context(), desk.Symbol)
	v.IndexFresh = idx.Fresh
	if idx.Price.IsPositive() {
		v.IndexPrice = idx.Price.String()
	}
	// Base and quote are funded independently now — no formula, just whatever
	// the admin deposited into each leg. Expose both as-is.
	if desk.Market == models.Spot {
		v.QuoteAsset = "USDT"
	} else {
		v.QuoteAsset = "USDC"
	}
	v.QuoteBalance = desk.QuoteAmount
	v.BaseBalance = desk.BaseAmount
	return v
}

func (s *Server) handleMMList(w http.ResponseWriter, r *http.Request) {
	desks, err := s.store.ListMM(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "failed to load market makers")
		return
	}
	views := make([]mmDeskView, 0, len(desks))
	for i := range desks {
		views = append(views, s.deskView(r, &desks[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"marketMakers": views})
}

type mmCreateRequest struct {
	Base   string            `json:"base"`
	Market models.Market     `json:"market"`
	Symbol string            `json:"symbol"`
	Config map[string]string `json:"config"`
}

func (s *Server) handleMMCreate(w http.ResponseWriter, r *http.Request) {
	var req mmCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	desk, err := s.mm.Create(r.Context(), req.Base, req.Market, req.Symbol, req.Config)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, s.deskView(r, desk))
}

func (s *Server) handleMMGet(w http.ResponseWriter, r *http.Request) {
	desk, err := s.store.GetMM(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "market maker not found")
		return
	}
	writeJSON(w, http.StatusOK, s.deskView(r, desk))
}

type mmFundRequest struct {
	Asset  string `json:"asset"` // "base" | "quote"
	Amount string `json:"amount"`
	Note   string `json:"note"`
}

func (s *Server) handleMMDeposit(w http.ResponseWriter, r *http.Request) {
	amount, req, ok := decodeFund(w, r)
	if !ok {
		return
	}
	desk, err := s.mm.Deposit(r.Context(), r.PathValue("id"), req.Asset, amount, adminID(r), req.Note)
	if err != nil {
		s.writeMMErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.deskView(r, desk))
}

func (s *Server) handleMMWithdraw(w http.ResponseWriter, r *http.Request) {
	amount, req, ok := decodeFund(w, r)
	if !ok {
		return
	}
	desk, err := s.mm.Withdraw(r.Context(), r.PathValue("id"), req.Asset, amount, adminID(r), req.Note)
	if err != nil {
		s.writeMMErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.deskView(r, desk))
}

type mmEnableRequest struct {
	Enabled bool `json:"enabled"`
}

func (s *Server) handleMMEnable(w http.ResponseWriter, r *http.Request) {
	var req mmEnableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.mm.SetEnabled(r.Context(), r.PathValue("id"), req.Enabled); err != nil {
		s.writeMMErr(w, err)
		return
	}
	desk, err := s.store.GetMM(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "market maker not found")
		return
	}
	writeJSON(w, http.StatusOK, s.deskView(r, desk))
}

func (s *Server) handleMMDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.mm.Delete(r.Context(), r.PathValue("id")); err != nil {
		s.writeMMErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *Server) handleMMHistory(w http.ResponseWriter, r *http.Request) {
	entries, err := s.store.FundingHistory(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "failed to load funding history")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"history": entries})
}

func (s *Server) handleMMOrders(w http.ResponseWriter, r *http.Request) {
	orders, err := s.mm.OpenOrders(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeMMErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"orders": orders})
}

func (s *Server) handleMMConfig(w http.ResponseWriter, r *http.Request) {
	var config map[string]string
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	desk, err := s.mm.UpdateConfig(r.Context(), r.PathValue("id"), config)
	if err != nil {
		s.writeMMErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.deskView(r, desk))
}

func (s *Server) handleMMStartAll(w http.ResponseWriter, r *http.Request) {
	if err := s.mm.SetAllEnabled(r.Context(), true); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	s.handleMMList(w, r)
}

func (s *Server) handleMMStopAll(w http.ResponseWriter, r *http.Request) {
	if err := s.mm.SetAllEnabled(r.Context(), false); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	s.handleMMList(w, r)
}

// decodeFund parses and validates a fund request body. asset must say which
// leg of the desk the amount applies to — "base" (e.g. BTC, ETH) or "quote"
// (e.g. USDT, USDC) — since the two are funded independently.
func decodeFund(w http.ResponseWriter, r *http.Request) (decimal.Decimal, mmFundRequest, bool) {
	var req mmFundRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return decimal.Zero, req, false
	}
	if req.Asset != "base" && req.Asset != "quote" {
		writeErr(w, http.StatusBadRequest, `asset must be "base" or "quote"`)
		return decimal.Zero, req, false
	}
	amount, err := decimal.NewFromString(req.Amount)
	if err != nil || !amount.IsPositive() {
		writeErr(w, http.StatusBadRequest, "amount must be a positive decimal")
		return decimal.Zero, req, false
	}
	return amount, req, true
}

// writeMMErr maps MM service/store errors to HTTP statuses.
func (s *Server) writeMMErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrMMNotFound):
		writeErr(w, http.StatusNotFound, "market maker not found")
	case errors.Is(err, store.ErrInsufficientFunds):
		writeErr(w, http.StatusConflict, "insufficient available funds; capital is reserved behind live quotes")
	default:
		writeErr(w, http.StatusBadRequest, err.Error())
	}
}
