package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestExpiredLeaseCannotCommitAndRunCompletes(t *testing.T) {
	// The test requires a real MySQL instance because lease expiry and
	// completion are both validated through database updates.
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
	// Start one three-step workflow and keep the run ID for cleanup below.
	id, err := e.Start(ctx, "text-pipeline", json.RawMessage(`{"text":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = e.DB.ExecContext(ctx, `DELETE FROM activity_tasks WHERE run_id=?`, id)
		_, _ = e.DB.ExecContext(ctx, `DELETE FROM history_events WHERE run_id=?`, id)
		_, _ = e.DB.ExecContext(ctx, `DELETE FROM workflow_runs WHERE id=?`, id)
	}()
	// Claim the first task, then force its lease to expire so another worker
	// can reclaim it with a new token.
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
	// A worker holding the expired token must not be able to advance the run.
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
	// Complete the reclaimed first task and the remaining two tasks normally.
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
	finalValue := run.CurrentValue
	checkpoints, err := e.ListCheckpoints(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) != 3 || checkpoints[0].Sequence != 1 || checkpoints[1].Sequence != 2 || checkpoints[2].Sequence != 3 {
		t.Fatalf("unexpected checkpoints: %+v", checkpoints)
	}
	if err := e.Reset(ctx, id, checkpoints[1].Sequence); err != nil {
		t.Fatal(err)
	}
	run, events, err = e.GetRun(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "running" || run.NextStep != 1 || run.CurrentValue != "HELLO" || len(events) != 6 || events[5].Type != "WorkflowReset" {
		t.Fatalf("reset state: run=%+v events=%+v", run, events)
	}
	// Reset while a worker holds the new task. The old task ID must be
	// invalidated, even when the run returns to the same step later.
	staleAfterReset, err := e.claim(ctx)
	if err != nil || staleAfterReset == nil || staleAfterReset.StepIndex != 1 {
		t.Fatalf("claim after reset: %v %+v", err, staleAfterReset)
	}
	if err := e.Reset(ctx, id, checkpoints[0].Sequence); err != nil {
		t.Fatal(err)
	}
	if err := e.complete(ctx, staleAfterReset, "incorrect", nil); err != nil {
		t.Fatal(err)
	}
	run, events, err = e.GetRun(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if run.NextStep != 0 || run.CurrentValue != "hello" || len(events) != 7 {
		t.Fatalf("stale completion changed reset state: run=%+v events=%d", run, len(events))
	}
	if err := e.Reset(ctx, id, 999999); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("invalid checkpoint: %v", err)
	}
	for step := 0; step < 3; step++ {
		claimed, err := e.claim(ctx)
		if err != nil || claimed == nil || claimed.StepIndex != step {
			t.Fatalf("claim rerun step %d: %v %+v", step, err, claimed)
		}
		output, err := execute(claimed.StepIndex, claimed.Value)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.complete(ctx, claimed, output, nil); err != nil {
			t.Fatal(err)
		}
	}
	run, _, err = e.GetRun(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" || run.CurrentValue != finalValue {
		t.Fatalf("rerun failed: %+v", run)
	}
}
