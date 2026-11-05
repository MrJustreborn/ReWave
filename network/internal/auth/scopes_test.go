package auth

import (
	"context"
	"fmt"
	"strings"
	"testing"

	openapi "github.com/getkin/kin-openapi/openapi3"
)

// The scope map must stay in exact parity with openapi.yaml: every
// operation declares exactly one uiAuth security requirement, and
// RequiredScopes mirrors the spec's scopes for every operation — nothing
// missing, nothing extra. This test replaces the TS codegen guarantee
// ("auth scopes are never hardcoded without verification").
func TestRequiredScopesMatchSpec(t *testing.T) {
	doc, err := openapi.NewLoader().LoadFromFile("../../openapi.yaml")
	if err != nil {
		t.Fatalf("load openapi.yaml: %v", err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatalf("validate openapi.yaml: %v", err)
	}

	type operation struct {
		key    string
		scopes []string
	}
	var operations []operation
	for path, item := range doc.Paths.Map() {
		byMethod := map[string]*openapi.Operation{
			"GET":    item.Get,
			"POST":   item.Post,
			"PUT":    item.Put,
			"DELETE": item.Delete,
		}
		for method, op := range byMethod {
			if op == nil {
				continue
			}
			scopes, err := uiAuthScopes(op)
			if err != nil {
				t.Fatalf("%s %s: %v", method, path, err)
			}
			operations = append(operations, operation{key: method + " " + path, scopes: scopes})
		}
	}
	if len(operations) == 0 {
		t.Fatal("no operations found in openapi.yaml — loader problem?")
	}

	seen := make(map[string]bool, len(operations))
	for _, op := range operations {
		if len(op.scopes) == 0 {
			t.Errorf("%s: spec declares no uiAuth scopes", op.key)
		}
		have, ok := RequiredScopes[op.key]
		if !ok {
			t.Errorf("%s: missing from RequiredScopes (add it)", op.key)
			continue
		}
		if fmt.Sprint(have) != fmt.Sprint(op.scopes) {
			t.Errorf("%s: RequiredScopes = %v, spec declares %v", op.key, have, op.scopes)
		}
		seen[op.key] = true
	}
	for key := range RequiredScopes {
		if !seen[key] {
			t.Errorf("%s: in RequiredScopes but not in openapi.yaml (remove it)", key)
		}
	}
}

// uiAuthScopes extracts the scopes of the (single, required) uiAuth
// security requirement of an operation.
func uiAuthScopes(op *openapi.Operation) ([]string, error) {
	if op.Security == nil || len(*op.Security) != 1 {
		return nil, fmt.Errorf("must declare exactly one security requirement")
	}
	requirement := (*op.Security)[0]
	scopes, ok := requirement["uiAuth"]
	if !ok {
		return nil, fmt.Errorf("security requirement must use the uiAuth scheme")
	}
	return scopes, nil
}

func TestMatchRoute(t *testing.T) {
	tests := []struct {
		method, path string
		wantScopes   []string
		wantOK       bool
	}{
		{"GET", "/v1/channels", []string{"channels:read"}, true},
		{"POST", "/v1/channels", []string{"channels:write"}, true},
		{"GET", "/v1/channels/3f6bd1c2-9c5e-4d20-8a3d-6f2a5c9e7b11", []string{"channels:read"}, true},
		{"POST", "/v1/channels/abc/start", []string{"channels:write"}, true},
		{"GET", "/v1/channels/abc/playlist", []string{"channels:read"}, true},
		{"GET", "/v1/videos", []string{"videos:read"}, true},
		{"DELETE", "/v1/videos/abc", []string{"videos:write"}, true},
		{"GET", "/healthz", nil, false},
		{"GET", "/v1/unknown", nil, false},
		{"PATCH", "/v1/channels/abc", nil, false},
		{"GET", "/v1/channels/abc/extra", nil, false},
		{"POST", "/v1/channels/", nil, false},
	}
	for _, tt := range tests {
		scopes, ok := MatchRoute(tt.method, tt.path)
		if ok != tt.wantOK {
			t.Errorf("%s %s: ok = %v, want %v", tt.method, tt.path, ok, tt.wantOK)
			continue
		}
		if !tt.wantOK {
			continue
		}
		if strings.Join(scopes, ",") != strings.Join(tt.wantScopes, ",") {
			t.Errorf("%s %s: scopes = %v, want %v", tt.method, tt.path, scopes, tt.wantScopes)
		}
	}
}
