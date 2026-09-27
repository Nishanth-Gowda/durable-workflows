package routers_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"example.com/durable-workflows/backend/internal/engine"
	"example.com/durable-workflows/backend/internal/handlers"
	"example.com/durable-workflows/backend/internal/routers"
	"example.com/durable-workflows/backend/internal/service"
)

type fakeRuns struct {
	startErr error
	getErr   error
	resetErr error
	resetID  string
	sequence int
}

func (*fakeRuns) Workflows() []engine.Workflow { return []engine.Workflow{} }
func (f *fakeRuns) StartRun(context.Context, string, json.RawMessage) (string, error) {
	return "run-1", f.startErr
}
func (*fakeRuns) ListRuns(context.Context) ([]engine.Run, error) { return []engine.Run{}, nil }
func (f *fakeRuns) GetRun(_ context.Context, id string) (service.RunDetail, error) {
	return service.RunDetail{Run: engine.Run{ID: id}, Events: []engine.Event{}, Checkpoints: []engine.Checkpoint{}}, f.getErr
}
func (f *fakeRuns) ResetRun(_ context.Context, id string, sequence int) (service.RunDetail, error) {
	f.resetID, f.sequence = id, sequence
	return service.RunDetail{Run: engine.Run{ID: id}, Events: []engine.Event{}, Checkpoints: []engine.Checkpoint{}}, f.resetErr
}

func serve(f *fakeRuns, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	w := httptest.NewRecorder()
	routers.New(handlers.New(f)).ServeHTTP(w, r)
	return w
}

func TestRunRoutes(t *testing.T) {
	f := &fakeRuns{}
	for _, tc := range []struct {
		method, path, body string
		status             int
		contains           string
	}{
		{"GET", "/healthz", "", 200, `"status":"ok"`},
		{"GET", "/api/workflows", "", 200, `"workflows":[]`},
		{"POST", "/api/runs", `{"workflow_name":"text-pipeline","input":{"text":"hello"}}`, 201, `"run_id":"run-1"`},
		{"POST", "/api/runs", `{`, 400, `"error":"invalid JSON"`},
		{"GET", "/api/runs", "", 200, `"runs":[]`},
		{"GET", "/api/runs/run-1", "", 200, `"checkpoints":[]`},
		{"POST", "/api/runs/run-1/reset", `{"checkpoint_sequence":2}`, 200, `"run":{"id":"run-1"`},
		{"POST", "/api/runs/run-1/reset", `{"checkpoint_sequence":0}`, 400, `"error":"checkpoint_sequence must be a positive integer"`},
	} {
		w := serve(f, tc.method, tc.path, tc.body)
		if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.contains) {
			t.Errorf("%s %s: status=%d body=%s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
	if f.resetID != "run-1" || f.sequence != 2 {
		t.Fatalf("reset arguments: id=%q sequence=%d", f.resetID, f.sequence)
	}
}

func TestRunErrors(t *testing.T) {
	f := &fakeRuns{getErr: sql.ErrNoRows, resetErr: engine.ErrInvalidCheckpoint}
	if w := serve(f, "GET", "/api/runs/missing", ""); w.Code != 404 {
		t.Fatalf("missing run: %d %s", w.Code, w.Body.String())
	}
	if w := serve(f, "POST", "/api/runs/run-1/reset", `{"checkpoint_sequence":1}`); w.Code != 400 {
		t.Fatalf("invalid checkpoint: %d %s", w.Code, w.Body.String())
	}
	f.getErr = errors.New("database unavailable")
	if w := serve(f, "GET", "/api/runs/run-1", ""); w.Code != 500 || !strings.Contains(w.Body.String(), "database error") {
		t.Fatalf("database failure: %d %s", w.Code, w.Body.String())
	}
}

func TestCORSAndCacheHeaders(t *testing.T) {
	f := &fakeRuns{}
	h := routers.New(handlers.New(f))
	r := httptest.NewRequest("OPTIONS", "/api/runs", nil)
	r.Header.Set("Origin", "http://localhost:3000")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 204 || w.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Fatalf("preflight: status=%d headers=%v", w.Code, w.Header())
	}
	r = httptest.NewRequest("GET", "/api/runs", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("missing API cache header: %v", w.Header())
	}
}
