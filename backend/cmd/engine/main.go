package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"example.com/durable-workflows/backend/internal/engine"
	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrations embed.FS

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	e, err := engine.New(ctx, databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()
	schema, err := migrations.ReadFile("migrations/001_init.sql")
	if err != nil {
		log.Fatal(err)
	}
	tx, err := e.DB.Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(ctx)
	for _, statement := range strings.Split(string(schema), ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := tx.Exec(ctx, statement); err != nil {
			log.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	mode := os.Getenv("ENGINE_MODE")
	if mode == "" {
		mode = "all"
	}
	if mode != "all" && mode != "api" && mode != "worker" {
		log.Fatal("ENGINE_MODE must be all, api, or worker")
	}
	if mode != "api" {
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
		<-ctx.Done()
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /api/workflows", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"workflows": engine.Workflows()})
	})
	mux.HandleFunc("POST /api/runs", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			WorkflowName string          `json:"workflow_name"`
			Input        json.RawMessage `json:"input"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		id, err := e.Start(r.Context(), body.WorkflowName, body.Input)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"run_id": id})
	})
	mux.HandleFunc("GET /api/runs", func(w http.ResponseWriter, r *http.Request) {
		runs, err := e.ListRuns(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "database error")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
	})
	mux.HandleFunc("GET /api/runs/{id}", func(w http.ResponseWriter, r *http.Request) {
		run, events, err := e.GetRun(r.Context(), r.PathValue("id"))
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "database error")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"run": run, "events": events})
	})
	server := &http.Server{Addr: ":8080", Handler: cors(mux), ReadHeaderTimeout: 5 * time.Second}
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

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "http://localhost:3000" || origin == "http://127.0.0.1:3000" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
