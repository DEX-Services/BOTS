// Package config loads bots-service configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for the bots service.
type Config struct {
	Port              string
	JWTSecret         string
	EngineURL         string
	PostgresURI       string
	AllowedOrigins    map[string]bool
	MarketDataPollMs  int
	EngineConcurrency int
}

// Load reads configuration from environment variables (and .env if present).
func Load() (Config, error) {
	c := Config{
		Port:              getenv("BOTS_PORT", "8082"),
		JWTSecret:        os.Getenv("JWT_SECRET"),
		EngineURL:        getenv("ENGINE_URL", "http://localhost:8080"),
		PostgresURI:      postgresURI(),
		AllowedOrigins:   parseOrigins(os.Getenv("BOTS_ALLOWED_ORIGINS")),
		MarketDataPollMs: getenvInt("MARKETDATA_POLL_MS", 800),
		EngineConcurrency: getenvInt("ENGINE_MAX_CONCURRENCY", 256),
	}
	if c.JWTSecret == "" {
		return c, fmt.Errorf("JWT_SECRET is required (must match Dex-Backend)")
	}
	if c.PostgresURI == "" {
		return c, fmt.Errorf("POSTGRES_SERVICE_URI or POSTGRES_HOST/USER/PASSWORD/DB is required")
	}
	return c, nil
}

// MarketDataPoll returns the configured poll interval as a Duration.
func (c Config) MarketDataPoll() time.Duration {
	if c.MarketDataPollMs <= 0 {
		return 800 * time.Millisecond
	}
	return time.Duration(c.MarketDataPollMs) * time.Millisecond
}

func postgresURI() string {
	if uri := os.Getenv("POSTGRES_SERVICE_URI"); uri != "" {
		return uri
	}
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		return ""
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=require",
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_PASSWORD"),
		host,
		getenv("POSTGRES_PORT", "19333"),
		os.Getenv("POSTGRES_DB"),
	)
}

func parseOrigins(s string) map[string]bool {
	m := map[string]bool{}
	for _, o := range strings.Split(s, ",") {
		if o = strings.TrimSpace(o); o != "" {
			m[o] = true
		}
	}
	return m
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getenvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
