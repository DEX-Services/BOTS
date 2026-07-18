package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/dex/bots/internal/auth"
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
}

// NewServer builds the API server.
func NewServer(st *store.Store, mgr *runtime.Manager, v *auth.Verifier) *Server {
	return &Server{store: st, manager: mgr, auth: v}
}

// Routes returns the HTTP mux with all endpoints wired. Public routes
// (templates, marketplace) allow unauthenticated access; everything else
// requires a valid dex_session JWT.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /bots/templates", methodGuard(http.MethodGet, s.handleTemplates))
	mux.HandleFunc("GET /bots/marketplace", methodGuard(http.MethodGet, s.handleMarketplace))

	mux.HandleFunc("GET /bots", s.requireAuth(methodGuard(http.MethodGet, s.handleList)))
	mux.HandleFunc("POST /bots", s.requireAuth(methodGuard(http.MethodPost, s.handleCreate)))

	mux.HandleFunc("GET /bots/{id}", s.requireAuth(methodGuard(http.MethodGet, s.handleGet)))
	mux.HandleFunc("POST /bots/{id}/start", s.requireAuth(methodGuard(http.MethodPost, s.handleStart)))
	mux.HandleFunc("POST /bots/{id}/stop", s.requireAuth(methodGuard(http.MethodPost, s.handleStop)))
	mux.HandleFunc("DELETE /bots/{id}", s.requireAuth(methodGuard(http.MethodDelete, s.handleDelete)))
	mux.HandleFunc("POST /bots/{id}/copy", s.requireAuth(methodGuard(http.MethodPost, s.handleCopy)))

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
	if mkt != models.Spot && mkt != models.Futures {
		return errInvalid("market must be SPOT or FUTURES")
	}
	if strings.HasPrefix(strategyKey, "spot_") && mkt != models.Spot {
		return errInvalid("this strategy is spot-only")
	}
	if strings.HasPrefix(strategyKey, "futures_") && mkt != models.Futures {
		return errInvalid("this strategy is futures-only")
	}
	return nil
}

type validationErr struct{ msg string }

func (e validationErr) Error() string { return e.msg }
func errInvalid(msg string) error      { return validationErr{msg: msg} }
