package main

import (
	"context"
	"embed"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"example.com/durable-workflows/backend/internal/engine"
	"example.com/durable-workflows/backend/internal/handlers"
	"example.com/durable-workflows/backend/internal/routers"
	"example.com/durable-workflows/backend/internal/service"
)

// Embed migrations so the engine can initialize its schema without depending
// on files being present in the process's working directory at runtime.
//
//go:embed migrations/*.sql
var migrations embed.FS

func main() {
	// The database is required before either the worker or API can start.
	databaseURL := os.Getenv("MYSQL_DSN")
	if databaseURL == "" {
		log.Fatal("MYSQL_DSN is required")
	}
	// One cancellation signal is shared by the worker, HTTP server, and database
	// calls so every component begins shutting down together.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	e, err := engine.New(ctx, databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()
	// Apply the embedded schema on startup. Each migration statement is executed
	// separately because the database driver does not receive a multi-statement
	// script here.
	schema, err := migrations.ReadFile("migrations/001_init.sql")
	if err != nil {
		log.Fatal(err)
	}
	for _, statement := range strings.Split(string(schema), ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := e.DB.ExecContext(ctx, statement); err != nil {
			log.Fatal(err)
		}
	}
	// The default process runs both roles; api and worker modes support separate
	// deployments when the roles need to scale independently.
	mode := os.Getenv("ENGINE_MODE")
	if mode == "" {
		mode = "all"
	}
	if mode != "all" && mode != "api" && mode != "worker" {
		log.Fatal("ENGINE_MODE must be all, api, or worker")
	}
	if mode != "api" {
		// A configured delay pauses after a task is claimed, making lease expiry
		// and crash recovery observable during local experiments.
		var activityDelay time.Duration
		if value := os.Getenv("WORKER_ACTIVITY_DELAY"); value != "" {
			activityDelay, err = time.ParseDuration(value)
			if err != nil {
				log.Fatal("invalid WORKER_ACTIVITY_DELAY: ", err)
			}
		}
		go func() {
			if err := e.Work(ctx, activityDelay); err != nil && ctx.Err() == nil {
				log.Printf("worker stopped: %v", err)
				stop()
			}
		}()
	}
	if mode == "worker" {
		// A worker has no HTTP listener, so it remains alive until cancellation.
		<-ctx.Done()
		return
	}
	server := &http.Server{Addr: ":8080", Handler: routers.New(handlers.New(service.New(e))), ReadHeaderTimeout: 5 * time.Second}
	// Shutdown is bounded so an unresponsive connection cannot keep the process
	// alive indefinitely after the shared context has been cancelled.
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("API listening on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
