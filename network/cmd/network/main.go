// Command network is the ReWave network — the master and manager of the
// virtual TV network (channel lifecycle, media library, REST API).
//
// Usage:
//
//	network                 serve (default)
//	network migrate         apply pending database migrations
//	network migrate-status  list applied/pending migrations
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"rewave/network/internal/apperr"
	"rewave/network/internal/auth"
	"rewave/network/internal/config"
	"rewave/network/internal/db"
	"rewave/network/internal/httpapi"
	"rewave/network/internal/httpapi/gen"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "network:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	command := "serve"
	if len(args) > 0 {
		command = args[0]
	}
	switch command {
	case "serve":
		return serve()
	case "migrate":
		return migrateCommand()
	case "migrate-status":
		return migrateStatusCommand()
	default:
		return fmt.Errorf("unknown command %q (usage: network [serve|migrate|migrate-status])", command)
	}
}

func serve() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	setupLogging(cfg)

	d, err := openDatabase(cfg)
	if err != nil {
		return err
	}
	defer d.Close()

	ctx := context.Background()
	applied, err := db.ApplyMigrations(ctx, d)
	if err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	if len(applied) > 0 {
		slog.Info("applied database migrations", "migrations", applied)
	}

	handler, authenticator, err := buildHandler(cfg)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.ListenAndServe() }()
	slog.Info("network listening", "addr", srv.Addr, "auth_mode", string(cfg.AuthMode))

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		authenticator.Stop()
		return nil
	case sig := <-stop:
		slog.Info("shutting down", "signal", sig.String())
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		authenticator.Stop()
		return d.Close()
	}
}

// buildHandler assembles the HTTP handler: CORS (outermost, so error
// responses carry CORS headers too), then authentication, then the
// generated OpenAPI routes, with /healthz outside the API.
func buildHandler(cfg config.Config) (http.Handler, *auth.Auth, error) {
	authenticator, err := auth.New(cfg)
	if err != nil {
		return nil, nil, err
	}
	api := gen.HandlerWithOptions(httpapi.New(), gen.StdHTTPServerOptions{
		// Path/query parameter validation failures follow the openapi
		// Error schema like every other error response.
		ErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			httpapi.WriteError(w, apperr.Validation(err.Error()))
		},
	})
	root := http.NewServeMux()
	root.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{"status":"ok"}`)
	})
	root.Handle("/", authenticator.Wrap(api))
	return httpapi.CORS(root), authenticator, nil
}

func migrateCommand() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	d, err := openDatabase(cfg)
	if err != nil {
		return err
	}
	defer d.Close()
	applied, err := db.ApplyMigrations(context.Background(), d)
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		fmt.Println("Nothing to apply")
	} else {
		fmt.Println("Applied:", strings.Join(applied, ", "))
	}
	return nil
}

func migrateStatusCommand() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	d, err := openDatabase(cfg)
	if err != nil {
		return err
	}
	defer d.Close()
	entries, err := db.MigrationStatus(context.Background(), d)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		appliedAt := ""
		if entry.AppliedAt != "" {
			appliedAt = " (applied " + entry.AppliedAt + ")"
		}
		fmt.Printf("%-8s %s%s\n", entry.State, entry.Name, appliedAt)
	}
	return nil
}

func openDatabase(cfg config.Config) (*sql.DB, error) {
	if dir := filepath.Dir(cfg.DBPath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return db.Open(cfg.DBPath)
}

func setupLogging(cfg config.Config) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})))
}
