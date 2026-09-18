package api

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// This service had no rate limiting anywhere (grep-verified across the whole
// platform). It has no local login endpoint of its own — admin identity is a
// shared-secret JWT minted by Dex-Backend, not brute-forceable here — so a
// single general-purpose tier is enough, unlike Dex-Backend's stricter
// /auth/* tier.
type limiterStore struct {
	mu       sync.Mutex
	limiters map[string]*rateEntry
	r        rate.Limit
	b        int
}

type rateEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

func newLimiterStore(r rate.Limit, b int) *limiterStore {
	s := &limiterStore{limiters: make(map[string]*rateEntry), r: r, b: b}
	go s.reapLoop()
	return s
}

func (s *limiterStore) allow(key string) bool {
	s.mu.Lock()
	entry, ok := s.limiters[key]
	if !ok {
		entry = &rateEntry{limiter: rate.NewLimiter(s.r, s.b)}
		s.limiters[key] = entry
	}
	entry.lastSeen = time.Now()
	limiter := entry.limiter
	s.mu.Unlock()
	return limiter.Allow()
}

// reapLoop bounds memory to roughly the number of distinct IPs seen in the
// last 10 minutes, not the lifetime of the process.
func (s *limiterStore) reapLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-10 * time.Minute)
		s.mu.Lock()
		for k, e := range s.limiters {
			if e.lastSeen.Before(cutoff) {
				delete(s.limiters, k)
			}
		}
		s.mu.Unlock()
	}
}

// RateLimit wraps next with a per-client-IP token bucket: 20 req/sec
// sustained, burst of 40 — matches Dex-Backend's general tier. Generous
// enough for the trade page's polling (my-bots refresh, marketplace) to
// never notice it, but enough to stop unthrottled bot-creation spam or
// scraping.
func RateLimit(next http.Handler) http.Handler {
	store := newLimiterStore(20, 40)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.RemoteAddr
		if host, _, err := net.SplitHostPort(key); err == nil {
			key = host
		}
		if !store.allow(key) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
