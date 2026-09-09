package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dex/bots/internal/auth"
	"github.com/dex/bots/internal/mm"
	"github.com/dex/bots/internal/models"
	"github.com/dex/bots/internal/runtime"
	"github.com/dex/bots/internal/store"
	"github.com/dex/bots/internal/strategy"
)

// Server is the bots HTTP API.
type Server struct {
	store   *store.Store
	manager *runtime.Manager
	auth    *auth.Verifier
	mm      *mm.Service
}

// NewServer builds the API server.
func NewServer(st *store.Store, mgr *runtime.Manager, v *auth.Verifier, mmSvc *mm.Service) *Server {
	return &Server{store: st, manager: mgr, auth: v, mm: mmSvc}
}

// Routes returns the HTTP mux with all endpoints wired. Public routes
// (templates, marketplace) allow unauthenticated access; everything else
// requires a valid dex_session JWT.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /bots/templates", methodGuard(http.MethodGet, s.handleTemplates))
	mux.HandleFunc("GET /bots/marketplace", methodGuard(http.MethodGet, s.handleMarketplace))

	// Public index price sourced from Redis (the price-fetcher's INDEX). Served
	// so the frontend header reads the same number the MM quotes and marks P/L
	// against, instead of an independent Binance REST poll.
	mux.HandleFunc("GET /index/{base}", methodGuard(http.MethodGet, s.handleIndex))
	// Same data over Server-Sent Events at the price-fetcher's 1s cadence, so
	// the frontend can drop its per-second GET /index/{base} polling. One
	// stream per open tab replaces that tab's 1 req/s with a single pushed
	// connection; the plain GET stays as the initial-load fallback.
	mux.HandleFunc("GET /index/stream", methodGuard(http.MethodGet, s.handleIndexStream))

	mux.HandleFunc("GET /bots", s.requireAuth(methodGuard(http.MethodGet, s.handleList)))
	mux.HandleFunc("POST /bots", s.requireAuth(methodGuard(http.MethodPost, s.handleCreate)))

	mux.HandleFunc("GET /bots/{id}", s.requireAuth(methodGuard(http.MethodGet, s.handleGet)))
	mux.HandleFunc("POST /bots/{id}/start", s.requireAuth(methodGuard(http.MethodPost, s.handleStart)))
	mux.HandleFunc("POST /bots/{id}/stop", s.requireAuth(methodGuard(http.MethodPost, s.handleStop)))
	mux.HandleFunc("DELETE /bots/{id}", s.requireAuth(methodGuard(http.MethodDelete, s.handleDelete)))
	mux.HandleFunc("POST /bots/{id}/copy", s.requireAuth(methodGuard(http.MethodPost, s.handleCopy)))

	// ----- admin: market-maker desks -----
	// Admin identity is a shared-secret session JWT (uid="admin") issued by
	// Dex-Backend; requireAdmin gates every desk route.
	mux.HandleFunc("GET /admin/mm", s.requireAdmin(methodGuard(http.MethodGet, s.handleMMList)))
	mux.HandleFunc("POST /admin/mm", s.requireAdmin(methodGuard(http.MethodPost, s.handleMMCreate)))
	mux.HandleFunc("POST /admin/mm/start-all", s.requireAdmin(methodGuard(http.MethodPost, s.handleMMStartAll)))
	mux.HandleFunc("POST /admin/mm/stop-all", s.requireAdmin(methodGuard(http.MethodPost, s.handleMMStopAll)))
	mux.HandleFunc("GET /admin/mm/{id}", s.requireAdmin(methodGuard(http.MethodGet, s.handleMMGet)))
	mux.HandleFunc("DELETE /admin/mm/{id}", s.requireAdmin(methodGuard(http.MethodDelete, s.handleMMDelete)))
	mux.HandleFunc("PATCH /admin/mm/{id}", s.requireAdmin(methodGuard(http.MethodPatch, s.handleMMConfig)))
	mux.HandleFunc("POST /admin/mm/{id}/deposit", s.requireAdmin(methodGuard(http.MethodPost, s.handleMMDeposit)))
	mux.HandleFunc("POST /admin/mm/{id}/withdraw", s.requireAdmin(methodGuard(http.MethodPost, s.handleMMWithdraw)))
	mux.HandleFunc("POST /admin/mm/{id}/enable", s.requireAdmin(methodGuard(http.MethodPost, s.handleMMEnable)))
	mux.HandleFunc("GET /admin/mm/{id}/orders", s.requireAdmin(methodGuard(http.MethodGet, s.handleMMOrders)))
	mux.HandleFunc("GET /admin/mm/{id}/history", s.requireAdmin(methodGuard(http.MethodGet, s.handleMMHistory)))

	return mux
}

// requireAuth wraps a handler with JWT verification (no public fallback). It
// accepts and returns http.HandlerFunc so it composes with methodGuard and
// can be registered with mux.HandleFunc.
func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth.Middleware(s.auth, false, next).ServeHTTP(w, r)
	}
}

// requireAdmin wraps a handler with JWT verification AND an admin-identity
// check. The admin session is a shared-secret JWT minted by Dex-Backend with
// uid="admin"; any other identity is rejected 403.
func (s *Server) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	guarded := func(w http.ResponseWriter, r *http.Request) {
		claims := auth.FromRequest(r)
		if claims == nil || claims.UserID != "admin" {
			writeErr(w, http.StatusForbidden, "admin access required")
			return
		}
		next(w, r)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		auth.Middleware(s.auth, false, http.HandlerFunc(guarded)).ServeHTTP(w, r)
	}
}

// ----- public -----

func (s *Server) handleTemplates(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"templates": strategy.Templates()})
}

func (s *Server) handleMarketplace(w http.ResponseWriter, r *http.Request) {
	strat := r.URL.Query().Get("strategy")
	mkt := r.URL.Query().Get("market")
	bots, err := s.store.ListPublic(r.Context(), strat, mkt)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "failed to load marketplace")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"bots": bots})
}

// handleIndex serves the Redis-backed INDEX price for a base asset (e.g. BTC).
// This is the same snapshot the MM quotes and marks P/L against, exposed so the
// frontend header stays consistent with the desk and the engine's mark price.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	// NOT upper-cased: this ticker is used verbatim as the Redis key
	// suffix (via IndexSnapshotExact). Most tickers price-fetcher writes
	// ARE upper-case (BTC, EURUSD, GOLD), so callers sending those still
	// work identically to before — but Live-Rates.com stock tickers are
	// stored with a case-sensitive suffix ("AAPL.us"), and forcing upper
	// case here used to make every stock lookup silently miss
	// ("price:AAPL.US" was never a real key).
	base := strings.TrimSpace(r.PathValue("base"))
	if base == "" {
		writeErr(w, http.StatusBadRequest, "base required")
		return
	}
	snap := s.manager.IndexSnapshotExact(r.Context(), base)
	writeJSON(w, http.StatusOK, map[string]any{
		"base":          base,
		"price":         snap.Price.String(),
		"fresh":         snap.Fresh,
		"ageMs":         snap.AgeMs,
		"changePercent": snap.ChangePercent,
		"high":          snap.High24h,
		"low":           snap.Low24h,
		"quoteVolume":   snap.QuoteVolume,
	})
}

// handleIndexStream serves the same Redis-backed index snapshot as handleIndex
// over Server-Sent Events: one "data:" frame per second (the price-fetcher's
// publication cadence), for as long as the client keeps the connection open.
// The frontend's useIndexPrice consumes this instead of re-GETting the REST
// endpoint every second — the read happens once per second server-side no
// matter how many tabs are watching, and each tab's marginal cost is one
// buffered write.
func (s *Server) handleIndexStream(w http.ResponseWriter, r *http.Request) {
	// NOT upper-cased: same exact-ticker lookup as handleIndex (Live-Rates
	// stock tickers keep their case-sensitive ".us" suffix in Redis keys).
	base := strings.TrimSpace(r.URL.Query().Get("base"))
	if base == "" {
		writeErr(w, http.StatusBadRequest, "base required")
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	writeOne := func() bool {
		snap := s.manager.IndexSnapshotExact(r.Context(), base)
		payload, err := json.Marshal(map[string]any{
			"base":          base,
			"price":         snap.Price.String(),
			"fresh":         snap.Fresh,
			"ageMs":         snap.AgeMs,
			"changePercent": snap.ChangePercent,
			"high":          snap.High24h,
			"low":           snap.Low24h,
			"quoteVolume":   snap.QuoteVolume,
		})
		if err != nil {
			return true // encoding a flat map cannot fail in practice; keep the stream alive
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
			return false // client disconnected
		}
		fl.Flush()
		return true
	}

	// First frame immediately so the UI renders a price without waiting a
	// full interval, then one frame per publication cycle.
	if !writeOne() {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if !writeOne() {
				return
			}
		}
	}
}

// ----- authed -----

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	claims := auth.FromRequest(r)
	bots, err := s.store.ListByUser(r.Context(), claims.UserID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "failed to load bots")
		return
	}
	for i := range bots {
		bots[i].IsRunning = s.manager.IsRunning(bots[i].ID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"bots": bots})
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	claims := auth.FromRequest(r)
	var req models.CreateBotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	req.Symbol = strings.ToUpper(strings.TrimSpace(req.Symbol))
	if req.Name == "" {
		writeErr(w, http.StatusBadRequest, "name is required")
		return
	}
	if !strategy.IsAvailable(req.Strategy) {
		writeErr(w, http.StatusBadRequest, "strategy not available")
		return
	}
	if err := validateMarketStrategy(req.Strategy, req.Market); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Investment == "" {
		req.Investment = "0"
	}
	bot := &models.Bot{
		Name: req.Name, Strategy: req.Strategy, Market: req.Market, Symbol: req.Symbol,
		Investment: req.Investment, Config: req.Config, IsPublic: req.IsPublic,
		UserID: claims.UserID, WalletAddress: claims.WalletAddress,
	}
	// Validate config by attempting to build the strategy (discarded; the
	// manager rebuilds it on start).
	if _, err := strategy.Build(bot); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.store.Create(r.Context(), bot); err != nil {
		writeErr(w, http.StatusInternalServerError, "failed to create bot")
		return
	}
	writeJSON(w, http.StatusCreated, bot)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	claims := auth.FromRequest(r)
	bot, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "bot not found")
		return
	}
	if bot.UserID != claims.UserID {
		writeErr(w, http.StatusForbidden, "not your bot")
		return
	}
	bot.IsRunning = s.manager.IsRunning(bot.ID)
	writeJSON(w, http.StatusOK, bot)
}

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	claims := auth.FromRequest(r)
	id := r.PathValue("id")
	bot, err := s.store.Get(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "bot not found")
		return
	}
	if bot.UserID != claims.UserID {
		writeErr(w, http.StatusForbidden, "not your bot")
		return
	}
	if err := s.manager.Start(r.Context(), id); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	claims := auth.FromRequest(r)
	id := r.PathValue("id")
	bot, err := s.store.Get(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "bot not found")
		return
	}
	if bot.UserID != claims.UserID {
		writeErr(w, http.StatusForbidden, "not your bot")
		return
	}
	if err := s.manager.Stop(r.Context(), id); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "stopped"})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	claims := auth.FromRequest(r)
	id := r.PathValue("id")
	bot, err := s.store.Get(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "bot not found")
		return
	}
	if bot.UserID != claims.UserID {
		writeErr(w, http.StatusForbidden, "not your bot")
		return
	}
	_ = s.manager.Stop(r.Context(), id) // cancel orders + stop worker if running
	if err := s.store.Delete(r.Context(), id); err != nil {
		writeErr(w, http.StatusInternalServerError, "failed to delete bot")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *Server) handleCopy(w http.ResponseWriter, r *http.Request) {
	claims := auth.FromRequest(r)
	id := r.PathValue("id")
	src, err := s.store.Get(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "bot not found")
		return
	}
	if !src.IsPublic {
		writeErr(w, http.StatusForbidden, "bot is not public")
		return
	}
	copy := &models.Bot{
		Name:       "Copy of " + src.Name,
		Strategy:   src.Strategy,
		Market:     src.Market,
		Symbol:     src.Symbol,
		Investment: src.Investment,
		Config:     src.Config,
		IsPublic:   false,
		UserID:     claims.UserID,
		WalletAddress: claims.WalletAddress,
	}
	if _, err := strategy.Build(copy); err != nil {
		writeErr(w, http.StatusBadRequest, "source bot config invalid: "+err.Error())
		return
	}
	if err := s.store.Create(r.Context(), copy); err != nil {
		writeErr(w, http.StatusInternalServerError, "failed to create bot")
		return
	}
	writeJSON(w, http.StatusCreated, copy)
}

// validateMarketStrategy enforces that a strategy's market category matches.
func validateMarketStrategy(strategyKey string, mkt models.Market) error {
	if mkt != models.Spot && mkt != models.Futures && mkt != models.Options {
		return errInvalid("market must be SPOT, FUTURES, or OPTIONS")
	}
	if strings.HasPrefix(strategyKey, "spot_") && mkt != models.Spot {
		return errInvalid("this strategy is spot-only")
	}
	if strings.HasPrefix(strategyKey, "futures_") && mkt != models.Futures {
		return errInvalid("this strategy is futures-only")
	}
	if strings.HasPrefix(strategyKey, "options_") && mkt != models.Options {
		return errInvalid("this strategy is options-only")
	}
	return nil
}

type validationErr struct{ msg string }

func (e validationErr) Error() string { return e.msg }
func errInvalid(msg string) error      { return validationErr{msg: msg} }
