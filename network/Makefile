.PHONY: generate build test vet run adseeker tidy

# Regenerate internal/httpapi/gen from openapi.yaml (pinned oapi-codegen tool).
generate:
	go generate ./...

build: generate
	CGO_ENABLED=0 go build -o bin/network ./cmd/network

test:
	go test ./...

vet:
	go vet ./...

run: build
	./bin/network

# Ad-break detection from the command line:
#   make adseeker VIDEO=<path> [MIN_DURATION=<seconds>]
adseeker:
	go run ./cmd/adseeker $(VIDEO) --min-duration $(or $(MIN_DURATION),3600)

tidy:
	go mod tidy
