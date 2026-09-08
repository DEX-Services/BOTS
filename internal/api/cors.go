// Package api implements the bots-service HTTP API.
package api

import (
	"encoding/json"
	"net/http"
	"strings"
)

// CORS wraps a handler with credentialed CORS for the configured origins.
// The frontend calls the bots API with credentials: "include" (the dex_session
// cookie), so ACAO must reflect the specific origin and ACA-Credentials=true.
func CORS(allowed map[string]bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && (len(allowed) == 0 || allowed[origin]) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Vary", "Origin")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// writeJSON encodes v as JSON with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeErr writes a {error:...} JSON body.
func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// methodGuard restricts a handler to a single method.
func methodGuard(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			writeErr(w, http.StatusMethodNotAllowed, method+" only")
			return
		}
		next(w, r)
	}
}

// trimSlash removes a trailing slash from a path (for /bots/{id}/).
func trimSlash(p string) string { return strings.TrimRight(p, "/") }
