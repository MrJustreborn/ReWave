// Package auth protects the REST API. The network is a pure OAuth2
// resource server: in AUTH_MODE=jwks it verifies bearer tokens against a
// JWKS endpoint (issuer/audience optional) and enforces the scopes
// required per operation; AUTH_MODE=off is the local development bypass.
package auth

import (
	"strings"
)

// RequiredScopes maps "METHOD /v1/..." route patterns — in the same
// {param} shape as the paths in openapi.yaml — to the OAuth2 scopes the
// operation requires.
//
// The scopes_are_specified_test verifies this map against openapi.yaml:
// every operation in the spec must appear here with exactly the scopes it
// declares, and nothing extra may be here. Keep spec and map in sync —
// the test fails the build otherwise.
var RequiredScopes = map[string][]string{
	"GET /v1/channels":                      {"channels:read"},
	"POST /v1/channels":                     {"channels:write"},
	"GET /v1/channels/{channelId}":          {"channels:read"},
	"PUT /v1/channels/{channelId}":          {"channels:write"},
	"DELETE /v1/channels/{channelId}":       {"channels:write"},
	"POST /v1/channels/{channelId}/start":   {"channels:write"},
	"POST /v1/channels/{channelId}/stop":    {"channels:write"},
	"POST /v1/channels/{channelId}/watch":   {"watch"},
	"GET /v1/channels/{channelId}/playlist": {"channels:read"},
	"GET /v1/videos":                        {"videos:read"},
	"POST /v1/videos":                       {"videos:write"},
	"GET /v1/videos/{videoId}":              {"videos:read"},
	"DELETE /v1/videos/{videoId}":           {"videos:write"},
}

// MatchRoute resolves the required scopes for a method/path pair.
// Path parameters (segments like {channelId} in the pattern) match any
// single non-empty path segment. The second return value reports whether
// the route is a known API operation; routes outside the API (e.g.
// /healthz) are not authenticated at all.
func MatchRoute(method, path string) ([]string, bool) {
	for pattern, scopes := range RequiredScopes {
		patternMethod, patternPath, ok := strings.Cut(pattern, " ")
		if !ok || patternMethod != method || !pathMatches(patternPath, path) {
			continue
		}
		return scopes, true
	}
	return nil, false
}

// pathMatches reports whether a concrete request path matches a pattern
// with {param} placeholders.
func pathMatches(pattern, path string) bool {
	patternParts := strings.Split(pattern, "/")
	pathParts := strings.Split(path, "/")
	if len(patternParts) != len(pathParts) {
		return false
	}
	for i, part := range patternParts {
		if strings.HasPrefix(part, "{") && strings.HasSuffix(part, "}") {
			if pathParts[i] == "" {
				return false
			}
			continue
		}
		if part != pathParts[i] {
			return false
		}
	}
	return true
}
