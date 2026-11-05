// Package httpapi hosts the REST API driving adapter: the generated
// OpenAPI types and server interface (in gen/), the handler
// implementation, error mapping and CORS.
package httpapi

// Regenerate internal/httpapi/gen from the authoritative contract
// ../../openapi.yaml. The output is gitignored and always produced by
// `go generate ./...` (Makefile and Dockerfile both run it).
//
//go:generate go tool oapi-codegen -package gen -generate types,std-http -o gen/gen.go ../../openapi.yaml
