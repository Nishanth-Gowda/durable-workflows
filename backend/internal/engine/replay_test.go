package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestBranchReplayUsesRecordedResult(t *testing.T) {
	name := "uppercase"
	events := []Event{
		{Type: "ActivityScheduled", StepName: &name, Details: json.RawMessage(`{"input":"hello"}`)},
		{Type: "ActivityCompleted", StepName: &name, Details: json.RawMessage(`{"output":"HELLO"}`)},
	}
	_, pending, index, err := replayBranch(events, "hello")
	if err != nil || pending == nil || pending.name != "reverse" || pending.input != "HELLO" || index != 1 {
		t.Fatalf("replay decision: pending=%+v index=%d err=%v", pending, index, err)
	}
	if _, _, _, err := replayBranch(events, "changed"); err == nil || !strings.Contains(err.Error(), "non-deterministic") {
		t.Fatalf("changed input should fail replay: %v", err)
	}
}

func TestBranchReplayAcrossEngineRestart(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN to run MySQL integration test")
	}
	ctx := context.Background()
	for _, tc := range []struct{ input, chosen string }{{"hello", "reverse"}, {"a longer text input", "sha256"}} {
		t.Run(tc.chosen, func(t *testing.T) {
			first, err := New(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			schema, err := os.ReadFile("../../cmd/engine/migrations/001_init.sql")
			if err != nil {
				t.Fatal(err)
			}
			for _, statement := range strings.Split(string(schema), ";") {
				if strings.TrimSpace(statement) != "" {
					if _, err := first.DB.ExecContext(ctx, statement); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := first.EnsureReplaySchema(ctx); err != nil {
				t.Fatal(err)
			}
			payload, _ := json.Marshal(map[string]string{"text": tc.input})
			id, err := first.Start(ctx, branchingWorkflow.Name, payload)
			if err != nil {
				t.Fatal(err)
			}
			var cleanupDB *sql.DB = first.DB
			var second *Engine
			defer func() {
				_, _ = cleanupDB.ExecContext(ctx, `DELETE FROM activity_tasks WHERE run_id=?`, id)
				_, _ = cleanupDB.ExecContext(ctx, `DELETE FROM history_events WHERE run_id=?`, id)
				_, _ = cleanupDB.ExecContext(ctx, `DELETE FROM workflow_runs WHERE id=?`, id)
				if second != nil {
					second.Close()
				}
			}()
			if processed, err := first.processBranchWorkflowTask(ctx); err != nil || !processed {
				t.Fatalf("initial workflow task: processed=%v err=%v", processed, err)
			}
			activity, err := first.claim(ctx)
			if err != nil || activity == nil || activity.ActivityName != "uppercase" {
				t.Fatalf("first activity: task=%+v err=%v", activity, err)
			}
			output, err := executeBranch(activity)
			if err != nil {
				t.Fatal(err)
			}
			if err := first.complete(ctx, activity, output, nil); err != nil {
				t.Fatal(err)
			}
			var pending bool
			if err := first.DB.QueryRowContext(ctx, `SELECT replay_pending FROM workflow_runs WHERE id=?`, id).Scan(&pending); err != nil || !pending {
				t.Fatalf("completion did not durably wake replay: pending=%v err=%v", pending, err)
			}
			// No second activity is scheduled yet. Replace the whole engine now.
			var secondTasks int
			if err := first.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM activity_tasks WHERE run_id=? AND step_index=1`, id).Scan(&secondTasks); err != nil || secondTasks != 0 {
				t.Fatalf("second activity scheduled before replay: tasks=%d err=%v", secondTasks, err)
			}
			first.Close()
			second, err = New(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			cleanupDB = second.DB
			if processed, err := second.processBranchWorkflowTask(ctx); err != nil || !processed {
				t.Fatalf("replay after restart: processed=%v err=%v", processed, err)
			}
			activity, err = second.claim(ctx)
			if err != nil || activity == nil || activity.ActivityName != tc.chosen || activity.StepIndex != 1 {
				t.Fatalf("chosen activity: task=%+v err=%v", activity, err)
			}
			output, err = executeBranch(activity)
			if err != nil {
				t.Fatal(err)
			}
			if err := second.complete(ctx, activity, output, nil); err != nil {
				t.Fatal(err)
			}
			if processed, err := second.processBranchWorkflowTask(ctx); err != nil || !processed {
				t.Fatalf("final workflow task: processed=%v err=%v", processed, err)
			}
			run, events, err := second.GetRun(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if run.Status != "completed" || len(events) != 6 {
				t.Fatalf("run after replay: status=%s events=%v", run.Status, events)
			}
			var firstAttempts, firstCount int
			if err := second.DB.QueryRowContext(ctx, `SELECT attempts FROM activity_tasks WHERE run_id=? AND step_index=0`, id).Scan(&firstAttempts); err != nil {
				t.Fatal(err)
			}
			for _, event := range events {
				if event.Type == "ActivityCompleted" && event.StepName != nil && *event.StepName == "uppercase" {
					firstCount++
				}
			}
			if firstAttempts != 1 || firstCount != 1 {
				t.Fatalf("first activity repeated: attempts=%d completions=%d", firstAttempts, firstCount)
			}
		})
	}
}
