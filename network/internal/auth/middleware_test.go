package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"

	"rewave/network/internal/config"
)

// authFixture serves a JWKS endpoint and can sign tokens with the
// published key.
type authFixture struct {
	server *httptest.Server
	key    *rsa.PrivateKey
}

func newAuthFixture(t *testing.T) *authFixture {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	pub, err := jwk.Import(&key.PublicKey)
	if err != nil {
		t.Fatalf("import public key: %v", err)
	}
	if err := pub.Set(jwk.KeyIDKey, "test-key"); err != nil {
		t.Fatalf("set kid: %v", err)
	}
	set := jwk.NewSet()
	if err := set.AddKey(pub); err != nil {
		t.Fatalf("add key to set: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(set)
	}))
	t.Cleanup(server.Close)
	return &authFixture{server: server, key: key}
}

// signToken builds a signed bearer token with the given scope claim,
// issuer and audience. The signing JWK carries the same kid as the
// published JWKS key, so WithKeySet can match it.
func (f *authFixture) signToken(t *testing.T, scope any, issuer, audience string) string {
	t.Helper()
	signingKey, err := jwk.Import(f.key)
	if err != nil {
		t.Fatalf("import signing key: %v", err)
	}
	if err := signingKey.Set(jwk.KeyIDKey, "test-key"); err != nil {
		t.Fatalf("set kid: %v", err)
	}
	builder := jwt.NewBuilder().Claim("scope", scope)
	if issuer != "" {
		builder = builder.Issuer(issuer)
	}
	if audience != "" {
		builder = builder.Audience([]string{audience})
	}
	token, err := builder.Build()
	if err != nil {
		t.Fatalf("build token: %v", err)
	}
	signed, err := jwt.Sign(token, jwt.WithKey(jwa.RS256(), signingKey))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return string(signed)
}

func jwksConfig(fixture *authFixture, issuer, audience string) config.Config {
	return config.Config{
		AuthMode: config.AuthJWKS,
		JWKSURL:  fixture.server.URL,
		Issuer:   issuer,
		Audience: audience,
	}
}

func TestOffModeAuthenticatesNothing(t *testing.T) {
	a, err := New(config.Config{AuthMode: config.AuthOff})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reached := false
	handler := a.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/channels", nil))
	if rec.Code != http.StatusOK || !reached {
		t.Fatalf("code = %d, reached = %v", rec.Code, reached)
	}
}

func TestJWKSAuth(t *testing.T) {
	fixture := newAuthFixture(t)
	a, err := New(jwksConfig(fixture, "https://idp.example.com", "rewave-network"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(a.Stop)

	newHandler := func() (http.Handler, *bool) {
		reached := false
		return a.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			reached = true
			w.WriteHeader(http.StatusOK)
		})), &reached
	}

	t.Run("missing token is 401", func(t *testing.T) {
		handler, reached := newHandler()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/channels", nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("code = %d, want 401", rec.Code)
		}
		if *reached {
			t.Fatal("handler must not be reached")
		}
		if challenge := rec.Header().Get("WWW-Authenticate"); challenge == "" {
			t.Fatal("expected WWW-Authenticate header")
		}
	})

	t.Run("garbage token is 401", func(t *testing.T) {
		handler, _ := newHandler()
		req := httptest.NewRequest(http.MethodGet, "/v1/channels", nil)
		req.Header.Set("Authorization", "Bearer not-a-token")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("code = %d, want 401", rec.Code)
		}
	})

	t.Run("valid token with required scope passes", func(t *testing.T) {
		handler, reached := newHandler()
		token := fixture.signToken(t, "channels:read some-other-scope", "https://idp.example.com", "rewave-network")
		req := httptest.NewRequest(http.MethodGet, "/v1/channels", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || !*reached {
			t.Fatalf("code = %d, reached = %v", rec.Code, *reached)
		}
	})

	t.Run("missing scope is 403", func(t *testing.T) {
		handler, reached := newHandler()
		token := fixture.signToken(t, "videos:read", "https://idp.example.com", "rewave-network")
		req := httptest.NewRequest(http.MethodGet, "/v1/channels", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("code = %d, want 403", rec.Code)
		}
		if *reached {
			t.Fatal("handler must not be reached")
		}
	})

	t.Run("array scope claim passes", func(t *testing.T) {
		handler, reached := newHandler()
		token := fixture.signToken(t, []any{"channels:read"}, "https://idp.example.com", "rewave-network")
		req := httptest.NewRequest(http.MethodGet, "/v1/channels", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || !*reached {
			t.Fatalf("code = %d, reached = %v", rec.Code, *reached)
		}
	})

	t.Run("wrong issuer is 401", func(t *testing.T) {
		handler, _ := newHandler()
		token := fixture.signToken(t, "channels:read", "https://evil.example.com", "rewave-network")
		req := httptest.NewRequest(http.MethodGet, "/v1/channels", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("code = %d, want 401", rec.Code)
		}
	})

	t.Run("wrong audience is 401", func(t *testing.T) {
		handler, _ := newHandler()
		token := fixture.signToken(t, "channels:read", "https://idp.example.com", "someone-else")
		req := httptest.NewRequest(http.MethodGet, "/v1/channels", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("code = %d, want 401", rec.Code)
		}
	})

	t.Run("non-API routes pass without token", func(t *testing.T) {
		handler, reached := newHandler()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		if rec.Code != http.StatusOK || !*reached {
			t.Fatalf("code = %d, reached = %v", rec.Code, *reached)
		}
	})
}
