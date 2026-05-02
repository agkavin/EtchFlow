package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all runtime configuration for EtchFlow.
// Loaded from environment variables or .env file via Viper.
type Config struct {
	DatabaseURL    string
	DBPoolMaxConns int
	HTTPPort       int
	LogLevel       string
	LogFormat      string
}

// Load reads configuration from environment variables (and optional .env file).
// Fails fast if DATABASE_URL is missing — the service cannot run without a database.
func Load() (*Config, error) {
	v := viper.New()

	// Defaults
	v.SetDefault("DB_POOL_MAX_CONNS", 10)
	v.SetDefault("HTTP_PORT", 8080)
	v.SetDefault("LOG_LEVEL", "info")
	v.SetDefault("LOG_FORMAT", "console")

	// Automatically read from environment variables
	v.AutomaticEnv()

	// Optionally load from .env file (not required — Docker passes env vars directly)
	v.SetConfigFile(".env")
	v.SetConfigType("env")
	// Ignore error if .env file not found — env vars from Docker compose are sufficient
	_ = v.ReadInConfig()

	// DATABASE_URL is required — fail fast
	dbURL := strings.TrimSpace(v.GetString("DATABASE_URL"))
	if dbURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required but not set. " +
			"Set it in .env or as an environment variable.")
	}

	return &Config{
		DatabaseURL:    dbURL,
		DBPoolMaxConns: v.GetInt("DB_POOL_MAX_CONNS"),
		HTTPPort:       v.GetInt("HTTP_PORT"),
		LogLevel:       v.GetString("LOG_LEVEL"),
		LogFormat:      v.GetString("LOG_FORMAT"),
	}, nil
}
