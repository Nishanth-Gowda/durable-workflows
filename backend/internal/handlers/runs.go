package handlers

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"

	"example.com/durable-workflows/backend/internal/engine"
)

func (h *Handlers) StartRun(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkflowName string          `json:"workflow_name"`
		Input        json.RawMessage `json:"input"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	id, err := h.runs.StartRun(r.Context(), body.WorkflowName, body.Input)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"run_id": id})
}

func (h *Handlers) ListRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := h.runs.ListRuns(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (h *Handlers) GetRun(w http.ResponseWriter, r *http.Request) {
	detail, err := h.runs.GetRun(r.Context(), r.PathValue("id"))
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (h *Handlers) ResetRun(w http.ResponseWriter, r *http.Request) {
	var body struct {
		CheckpointSequence *int `json:"checkpoint_sequence"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.CheckpointSequence == nil || *body.CheckpointSequence < 1 {
		writeError(w, http.StatusBadRequest, "checkpoint_sequence must be a positive integer")
		return
	}
	detail, err := h.runs.ResetRun(r.Context(), r.PathValue("id"), *body.CheckpointSequence)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			writeError(w, http.StatusNotFound, "run not found")
		case errors.Is(err, engine.ErrInvalidCheckpoint):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "database error")
		}
		return
	}
	writeJSON(w, http.StatusOK, detail)
}
