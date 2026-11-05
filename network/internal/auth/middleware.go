package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/lestrrat-go/httprc/v3"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"

	"rewave/network/internal/config"
)

// Auth is the API authentication layer. AUTH_MODE=off (local development)
// authenticates nothing; AUTH_MODE=jwks turns the network into an OAuth2
// resource server that verifies bearer tokens against a JWKS endpoint
// and enforces per-operation scopes.
type Auth struct {
	wrap func(http.Handler) http.Handler
	stop func()
}

// New builds the authentication layer for the given configuration.
func New(cfg config.Config) (*Auth, error) {
	if cfg.AuthMode == config.AuthOff {
		return &Auth{
			wrap: func(next http.Handler) http.Handler { return next },
			stop: func() {},
		}, nil
	}
	verifier, err := newJWKSVerifier(cfg)
	if err != nil {
		return nil, err
	}
	return &Auth{wrap: verifier.middleware, stop: verifier.stop}, nil
}

// Wrap returns next wrapped in the authentication middleware.
func (a *Auth) Wrap(next http.Handler) http.Handler { return a.wrap(next) }

// Stop releases background resources (JWKS refresh goroutines). Call once
// the HTTP server has shut down.
func (a *Auth) Stop() { a.stop() }

// jwksVerifier verifies bearer tokens against a JWKS endpoint, with
// automatic key-set refresh handled by jwk.Cache.
type jwksVerifier struct {
	jwksURL   string
	issuer    string
	audience  string
	keySet    *jwk.Cache
	ctxCancel context.CancelFunc
}

func newJWKSVerifier(cfg config.Config) (*jwksVerifier, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cache, err := jwk.NewCache(ctx, httprc.NewClient())
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create JWKS cache: %w", err)
	}
	if err := cache.Register(ctx, cfg.JWKSURL); err != nil {
		cancel()
		return nil, fmt.Errorf("register JWKS endpoint %s: %w", cfg.JWKSURL, err)
	}
	// Warm the cache so key fetch problems surface early; a failing IdP is
	// logged but does not abort startup — lookups retry on demand.
	if _, err := cache.Refresh(ctx, cfg.JWKSURL); err != nil {
		slog.Warn("initial JWKS fetch failed; will retry on demand",
			"url", cfg.JWKSURL, "error", err.Error())
	}
	return &jwksVerifier{
		jwksURL:   cfg.JWKSURL,
		issuer:    cfg.Issuer,
		audience:  cfg.Audience,
		keySet:    cache,
		ctxCancel: cancel,
	}, nil
}

func (v *jwksVerifier) stop() {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = v.keySet.Shutdown(shutdownCtx)
	v.ctxCancel()
}

func (v *jwksVerifier) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		required, isAPI := MatchRoute(r.Method, r.URL.Path)
		if !isAPI {
			next.ServeHTTP(w, r) // not an API operation (healthz, unknown) — no auth
			return
		}

		header := r.Header.Get("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			unauthorized(w, `Bearer realm="rewave-network"`, "Missing bearer token")
			return
		}

		granted, err := v.tokenScopes(r, strings.TrimPrefix(header, "Bearer "))
		if err != nil {
			unauthorized(w, `Bearer realm="rewave-network", error="invalid_token"`, err.Error())
			return
		}
		if missing := missingScopes(required, granted); len(missing) > 0 {
			forbidden(w, "Token is missing required scope(s): "+strings.Join(missing, " "))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// tokenScopes verifies the token against the JWKS and returns the scopes
// granted by its `scope` claim (space-separated string or array of
// strings, per RFC 8693 / common IdP practice).
func (v *jwksVerifier) tokenScopes(r *http.Request, token string) ([]string, error) {
	ctx := r.Context()
	set, err := v.keySet.Lookup(ctx, v.jwksURL)
	if err != nil {
		// Cold cache or a refresh hiccup — force one fetch, then retry.
		set, err = v.keySet.Refresh(ctx, v.jwksURL)
		if err != nil {
			return nil, fmt.Errorf("fetch JWKS: %w", err)
		}
	}
	options := []jwt.ParseOption{
		jwt.WithKeySet(set, jws.WithInferAlgorithmFromKey(true)),
		jwt.WithValidate(true),
	}
	if v.issuer != "" {
		options = append(options, jwt.WithIssuer(v.issuer))
	}
	if v.audience != "" {
		options = append(options, jwt.WithAudience(v.audience))
	}
	parsed, err := jwt.ParseRequest(withToken(r, token), options...)
	if err != nil {
		return nil, err
	}
	var claim any
	if err := parsed.Get("scope", &claim); err != nil {
		return nil, nil // no scope claim → no scopes granted
	}
	switch value := claim.(type) {
	case string:
		return strings.Fields(value), nil
	case []any:
		scopes := make([]string, 0, len(value))
		for _, entry := range value {
			if text, ok := entry.(string); ok {
				scopes = append(scopes, text)
			}
		}
		return scopes, nil
	default:
		return nil, nil
	}
}

// withToken rewrites the Authorization header so jwt.ParseRequest finds
// the raw token string.
func withToken(r *http.Request, token string) *http.Request {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+token)
	return r
}

func missingScopes(required, granted []string) []string {
	have := make(map[string]bool, len(granted))
	for _, scope := range granted {
		have[scope] = true
	}
	var missing []string
	for _, scope := range required {
		if !have[scope] {
			missing = append(missing, scope)
		}
	}
	return missing
}

func unauthorized(w http.ResponseWriter, wwwAuthenticate, message string) {
	w.Header().Set("WWW-Authenticate", wwwAuthenticate)
	writeErrorBody(w, http.StatusUnauthorized, message)
}

func forbidden(w http.ResponseWriter, message string) {
	writeErrorBody(w, http.StatusForbidden, message)
}

func writeErrorBody(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{Code: status, Message: message})
}
