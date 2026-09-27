package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func (e *Engine) Start(ctx context.Context, workflowName string, input json.RawMessage) (string, error) {
	if workflowName != textWorkflow.Name {
		return "", fmt.Errorf("unknown workflow: %s", workflowName)
	}
	var payload struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(input, &payload); err != nil || strings.TrimSpace(payload.Text) == "" {
		return "", errors.New("input.text must be a non-empty string")
	}
	id, err := randomID()
	if err != nil {
		return "", err
	}
	// Publish the run, its first history event, and its first task atomically.
	// A worker can never observe a run that lacks its initial task.
	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO workflow_runs (id,workflow_name,status,input,current_value) VALUES (?,?,'running',?,?)`, id, workflowName, input, payload.Text); err != nil {
		return "", err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO history_events (run_id,sequence,type,details) VALUES (?,1,'WorkflowStarted',?)`, id, input); err != nil {
		return "", err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO activity_tasks (run_id,step_index,state) VALUES (?,0,'pending')`, id); err != nil {
		return "", err
	}
	return id, tx.Commit()
}

func (e *Engine) GetRun(ctx context.Context, id string) (Run, []Event, error) {
	r, err := scanRun(e.DB.QueryRowContext(ctx, `SELECT `+runColumns+` FROM workflow_runs WHERE id=?`, id))
	if err != nil {
		return Run{}, nil, err
	}
	rows, err := e.DB.QueryContext(ctx, `SELECT sequence,type,step_name,details,occurred_at FROM history_events WHERE run_id=? ORDER BY sequence`, id)
	if err != nil {
		return Run{}, nil, err
	}
	defer rows.Close()
	events := []Event{}
	for rows.Next() {
		var ev Event
		var step sql.NullString
		if err := rows.Scan(&ev.Sequence, &ev.Type, &step, &ev.Details, &ev.OccurredAt); err != nil {
			return Run{}, nil, err
		}
		if step.Valid {
			ev.StepName = &step.String
		}
		events = append(events, ev)
	}
	return r, events, rows.Err()
}

func (e *Engine) ListRuns(ctx context.Context) ([]Run, error) {
	rows, err := e.DB.QueryContext(ctx, `SELECT `+runColumns+` FROM workflow_runs ORDER BY created_at DESC LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runs := []Run{}
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}
