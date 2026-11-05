# syntax=docker/dockerfile:1

FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Generated API code (internal/httpapi/gen) is gitignored and produced at
# build time from openapi.yaml via the pinned oapi-codegen tool.
RUN go generate ./... \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/network ./cmd/network

FROM debian:bookworm-slim
# ffmpeg + ffprobe are hard requirements: the network runs ad-break
# detection in-process (ingestion spawns ffmpeg/ffprobe).
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /out/network /app/network
EXPOSE 8080
ENTRYPOINT ["/app/network"]
