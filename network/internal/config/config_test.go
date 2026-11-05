package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir) // no .env present

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != 8080 {
		t.Errorf("Port = %d, want 8080", cfg.Port)
	}
	if cfg.Host != "0.0.0.0" {
		t.Errorf("Host = %q, want 0.0.0.0", cfg.Host)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want info", cfg.LogLevel)
	}
	if cfg.DBPath != "data/network.db" {
		t.Errorf("DBPath = %q", cfg.DBPath)
	}
	if cfg.MediaRoot != "/media-pool" || cfg.MediaHostPath != "/media-pool" {
		t.Errorf("media paths = %q/%q", cfg.MediaRoot, cfg.MediaHostPath)
	}
	if cfg.ChannelImage != "rewave-channel" {
		t.Errorf("ChannelImage = %q", cfg.ChannelImage)
	}
	if cfg.WatchKeepAliveSeconds != 300 {
		t.Errorf("WatchKeepAliveSeconds = %d, want 300", cfg.WatchKeepAliveSeconds)
	}
	if cfg.AuthMode != AuthOff {
		t.Errorf("AuthMode = %q, want off", cfg.AuthMode)
	}
}

func TestLoadDotEnv(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeFile(t, dir, ".env", "PORT=9090\n# comment\n\nMEDIA_ROOT=/data/pool\nAUTH_MODE=jwks\nAUTH_JWKS_URL=https://idp.example.com/.well-known/jwks.json\n")

	// Real environment variables win over .env.
	t.Setenv("MEDIA_ROOT", "/from-env")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != 9090 {
		t.Errorf("Port = %d, want 9090 from .env", cfg.Port)
	}
	if cfg.MediaRoot != "/from-env" {
		t.Errorf("MediaRoot = %q, want /from-env (env wins)", cfg.MediaRoot)
	}
	if cfg.AuthMode != AuthJWKS {
		t.Errorf("AuthMode = %q, want jwks", cfg.AuthMode)
	}
	if cfg.JWKSURL != "https://idp.example.com/.well-known/jwks.json" {
		t.Errorf("JWKSURL = %q", cfg.JWKSURL)
	}
}

func TestLoadJwksRequiresURL(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("AUTH_MODE", "jwks")
	t.Setenv("AUTH_JWKS_URL", "")

	if _, err := Load(); err == nil {
		t.Fatal("expected error for AUTH_MODE=jwks without AUTH_JWKS_URL")
	}

	t.Setenv("AUTH_JWKS_URL", "https://idp.example.com/.well-known/jwks.json")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.JWKSURL != "https://idp.example.com/.well-known/jwks.json" {
		t.Errorf("JWKSURL = %q", cfg.JWKSURL)
	}
}

func TestLoadRejectsBadValues(t *testing.T) {
	tests := []struct {
		name   string
		setEnv func(t *testing.T)
	}{
		{"bad port", func(t *testing.T) { t.Setenv("PORT", "not-a-number") }},
		{"zero port", func(t *testing.T) { t.Setenv("PORT", "0") }},
		{"bad log level", func(t *testing.T) { t.Setenv("LOG_LEVEL", "trace") }},
		{"bad auth mode", func(t *testing.T) { t.Setenv("AUTH_MODE", "magic") }},
		{"bad keepalive", func(t *testing.T) { t.Setenv("WATCH_KEEPALIVE_SECONDS", "-5") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Chdir(dir)
			tt.setEnv(t)
			if _, err := Load(); err == nil {
				t.Fatal("expected Load to fail")
			}
		})
	}
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
