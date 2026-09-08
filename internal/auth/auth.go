// Package auth verifies the user's session JWT (issued by Dex-Backend) so the
// bots service can identify which wallet a request belongs to. The bots
// service shares Dex-Backend's JWT_SECRET; it never issues its own tokens.
package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

const sessionCookie = "dex_session"

// Claims mirrors Dex-Backend's auth.Claims (uid + addr).
type Claims struct {
	UserID        string `json:"uid"`
	WalletAddress string `json:"addr"`
	jwt.RegisteredClaims
}

// Verifier validates session JWTs against a shared HMAC secret.
type Verifier struct {
	secret []byte
}

// NewVerifier builds a Verifier from the shared JWT secret.
func NewVerifier(secret string) *Verifier {
	return &Verifier{secret: []byte(secret)}
}

// Verify parses and validates a token, returning the claims on success.
func (v *Verifier) Verify(tokenString string) (*Claims, error) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return v.secret, nil
	})
	if err != nil || !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}
	return claims, nil
}

// tokenFromRequest extracts the JWT from the dex_session cookie, falling back
// to an Authorization: Bearer header (the same precedence Dex-Backend uses).
func tokenFromRequest(r *http.Request) string {
	if c, err := r.Cookie(sessionCookie); err == nil && c.Value != "" {
		return c.Value
	}
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	return ""
}

type contextKey struct{}

// Middleware authenticates the request and stores *Claims in the request
// context. It writes a JSON 401 on failure. When allowPublic is true and no
// token is present, the request proceeds with a nil claims value (for
// templates/marketplace endpoints that are browsable while logged out).
func Middleware(v *Verifier, allowPublic bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := tokenFromRequest(r)
		if tok == "" {
			if allowPublic {
				next.ServeHTTP(w, r)
				return
			}
			writeAuthError(w, "not authenticated")
			return
		}
		claims, err := v.Verify(tok)
		if err != nil {
			writeAuthError(w, "invalid session")
			return
		}
		next.ServeHTTP(w, r.WithContext(setClaims(r, claims)))
	})
}

// FromRequest returns the authenticated claims, or nil if the request is
// unauthenticated (only possible on allowPublic routes).
func FromRequest(r *http.Request) *Claims {
	if c, ok := r.Context().Value(contextKey{}).(*Claims); ok {
		return c
	}
	return nil
}

func setClaims(r *http.Request, c *Claims) context.Context {
	return context.WithValue(r.Context(), contextKey{}, c)
}

func writeAuthError(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
