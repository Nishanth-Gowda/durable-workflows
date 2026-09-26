package engine

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestExpiredLeaseCannotCommitAndRunCompletes(t *testing.T) {
	url := os.Getenv("TEST_MYSQL_DSN")
	if url == "" {
		t.Skip("set TEST_MYSQL_DSN to run MySQL integration test")
	}
	ctx := context.Background()
	e, err := New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	schema, err := os.ReadFile("../../cmd/engine/migrations/001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range strings.Split(string(schema), ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := e.DB.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	id, err := e.Start(ctx, "text-pipeline", json.RawMessage(`{"text":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = e.DB.ExecContext(ctx, `DELETE FROM activity_tasks WHERE run_id=?`, id)
		_, _ = e.DB.ExecContext(ctx, `DELETE FROM history_events WHERE run_id=?`, id)
		_, _ = e.DB.ExecContext(ctx, `DELETE FROM workflow_runs WHERE id=?`, id)
	}()
	stale, err := e.claim(ctx)
	if err != nil || stale == nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := e.DB.ExecContext(ctx, `UPDATE activity_tasks SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=?`, stale.ID); err != nil {
		t.Fatal(err)
	}
	fresh, err := e.claim(ctx)
	if err != nil || fresh == nil {
		t.Fatalf("reclaim: %v", err)
	}
	if fresh.Token == stale.Token {
		t.Fatal("reclaim reused lease token")
	}
	if err := e.complete(ctx, stale, "incorrect", nil); err != nil {
		t.Fatal(err)
	}
	run, events, err := e.GetRun(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if run.NextStep != 0 || len(events) != 1 {
		t.Fatalf("stale worker changed run: step=%d events=%d", run.NextStep, len(events))
	}
	for step := 0; step < 3; step++ {
		var task *task
		if step == 0 {
			task = fresh
		} else {
			task, err = e.claim(ctx)
			if err != nil || task == nil {
				t.Fatalf("claim step %d: %v", step, err)
			}
		}
		output, err := execute(task.StepIndex, task.Value)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.complete(ctx, task, output, nil); err != nil {
			t.Fatal(err)
		}
	}
	run, events, err = e.GetRun(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" || len(events) != 5 {
		t.Fatalf("status=%s events=%d", run.Status, len(events))
	}
}
