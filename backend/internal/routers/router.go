package routers

import (
	"net/http"

	"example.com/durable-workflows/backend/internal/handlers"
)

func New(h *handlers.Handlers) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.Health)
	mux.HandleFunc("GET /api/workflows", h.Workflows)
	mux.HandleFunc("POST /api/runs", h.StartRun)
	mux.HandleFunc("GET /api/runs", h.ListRuns)
	mux.HandleFunc("GET /api/runs/{id}", h.GetRun)
	mux.HandleFunc("POST /api/runs/{id}/reset", h.ResetRun)
	return cors(mux)
}
