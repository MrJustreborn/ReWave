// Package config turns environment variables (plus an optional .env file
// in the working directory) into a typed configuration. Real environment
// variables always win over .env values.
package config

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// AuthMode selects how incoming requests are authenticated.
type AuthMode string

const (
	// AuthOff is the local development bypass: no token is required.
	AuthOff AuthMode = "off"
	// AuthJWKS turns the network into an OAuth2 resource server that
	// verifies bearer tokens against a JWKS endpoint.
	AuthJWKS AuthMode = "jwks"
)

// Config is the network's runtime configuration. All fields mirror the
// environment variables documented in .env.example.
type Config struct {
	Port     int
	Host     string
	LogLevel slog.Level

	DBPath        string
	MediaRoot     string
	MediaHostPath string

	DockerSocket string
	ChannelImage string

	WatchKeepAliveSeconds int

	AuthMode AuthMode
	JWKSURL  string
	Issuer   string
	Audience string
}

// Load reads .env (if present), applies defaults and validation, and
// returns the configuration.
func Load() (Config, error) {
	loadDotEnv(".env")

	var cfg Config
	var err error

	if cfg.Port, err = intEnv("PORT", 8080); err != nil {
		return Config{}, err
	}
	cfg.Host = stringEnv("HOST", "0.0.0.0")
	if cfg.LogLevel, err = logLevelEnv("LOG_LEVEL", slog.LevelInfo); err != nil {
		return Config{}, err
	}

	cfg.DBPath = stringEnv("DB_PATH", "data/network.db")
	cfg.MediaRoot = stringEnv("MEDIA_ROOT", "/media-pool")
	// Default: the media pool lives at the same path on the Docker host
	// (only relevant once the network starts channel containers).
	cfg.MediaHostPath = stringEnv("MEDIA_HOST_PATH", cfg.MediaRoot)

	cfg.DockerSocket = stringEnv("DOCKER_SOCKET", "/var/run/docker.sock")
	cfg.ChannelImage = stringEnv("CHANNEL_IMAGE", "rewave-channel")

	if cfg.WatchKeepAliveSeconds, err = intEnv("WATCH_KEEPALIVE_SECONDS", 300); err != nil {
		return Config{}, err
	}

	switch mode := stringEnv("AUTH_MODE", string(AuthOff)); mode {
	case string(AuthOff):
		cfg.AuthMode = AuthOff
	case string(AuthJWKS):
		cfg.AuthMode = AuthJWKS
	default:
		return Config{}, fmt.Errorf("AUTH_MODE must be off or jwks, got %q", mode)
	}
	cfg.JWKSURL = stringEnv("AUTH_JWKS_URL", "")
	cfg.Issuer = stringEnv("AUTH_ISSUER", "")
	cfg.Audience = stringEnv("AUTH_AUDIENCE", "")
	if cfg.AuthMode == AuthJWKS && cfg.JWKSURL == "" {
		return Config{}, fmt.Errorf("AUTH_MODE=jwks requires AUTH_JWKS_URL to be set")
	}

	return cfg, nil
}

// loadDotEnv reads KEY=VALUE lines from path, if it exists. Values are
// only set for variables that are not already present in the environment.
func loadDotEnv(path string) {
	file, err := os.Open(path)
	if err != nil {
		return // missing .env is fine
	}
	defer file.Close()

	values := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}
		if key != "" {
			values[key] = value
		}
	}
	for key, value := range values {
		if _, ok := os.LookupEnv(key); !ok {
			os.Setenv(key, value)
		}
	}
}

// stringEnv returns the named variable or fallback when unset or empty.
func stringEnv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

// intEnv returns the named variable parsed as a positive integer.
func intEnv(name string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", name, value)
	}
	return parsed, nil
}

// logLevelEnv returns the named variable parsed as a slog level.
func logLevelEnv(name string, fallback slog.Level) (slog.Level, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	switch strings.ToLower(value) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("%s must be one of debug, info, warn, error, got %q", name, value)
	}
}
