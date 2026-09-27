package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

func checkpointFromEvent(ev Event) (Checkpoint, bool, error) {
	if ev.Type == "WorkflowStarted" {
		var input struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(ev.Details, &input); err != nil {
			return Checkpoint{}, false, err
		}
		name := textWorkflow.Steps[0].Name
		return Checkpoint{Sequence: ev.Sequence, NextStep: 0, StepName: &name, Value: input.Text}, true, nil
	}
	if ev.Type != "ActivityCompleted" || ev.StepName == nil {
		return Checkpoint{}, false, nil
	}
	for i, step := range textWorkflow.Steps {
		if step.Name != *ev.StepName {
			continue
		}
		if i+1 >= len(textWorkflow.Steps) {
			return Checkpoint{}, false, nil
		}
		var details struct {
			Output string `json:"output"`
		}
		if err := json.Unmarshal(ev.Details, &details); err != nil {
			return Checkpoint{}, false, err
		}
		name := textWorkflow.Steps[i+1].Name
		return Checkpoint{Sequence: ev.Sequence, NextStep: i + 1, StepName: &name, Value: details.Output}, true, nil
	}
	return Checkpoint{}, false, nil
}

// ListCheckpoints returns all restartable snapshots, including snapshots from
// earlier attempts. The final result is omitted because no activity follows it.
func (e *Engine) ListCheckpoints(ctx context.Context, id string) ([]Checkpoint, error) {
	rows, err := e.DB.QueryContext(ctx, `SELECT sequence,type,step_name,details,occurred_at FROM history_events WHERE run_id=? ORDER BY sequence`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	checkpoints := []Checkpoint{}
	for rows.Next() {
		var ev Event
		var step sql.NullString
		if err := rows.Scan(&ev.Sequence, &ev.Type, &step, &ev.Details, &ev.OccurredAt); err != nil {
			return nil, err
		}
		if step.Valid {
			ev.StepName = &step.String
		}
		checkpoint, ok, err := checkpointFromEvent(ev)
		if err != nil {
			return nil, err
		}
		if ok {
			checkpoints = append(checkpoints, checkpoint)
		}
	}
	return checkpoints, rows.Err()
}

var ErrInvalidCheckpoint = errors.New("checkpoint does not exist or cannot be restarted")

// Reset restores a saved snapshot and fences any worker holding an old task.
// Run state, tasks, and the audit event change in the same transaction.
func (e *Engine) Reset(ctx context.Context, id string, checkpointSequence int) error {
	tx, err := e.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var previousStatus, previousValue string
	var previousStep int
	if err := tx.QueryRowContext(ctx, `SELECT status,current_value,next_step FROM workflow_runs WHERE id=? FOR UPDATE`, id).Scan(&previousStatus, &previousValue, &previousStep); err != nil {
		return err
	}
	var ev Event
	var step sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT sequence,type,step_name,details FROM history_events WHERE run_id=? AND sequence=?`, id, checkpointSequence).Scan(&ev.Sequence, &ev.Type, &step, &ev.Details)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrInvalidCheckpoint
	}
	if err != nil {
		return err
	}
	if step.Valid {
		ev.StepName = &step.String
	}
	checkpoint, ok, err := checkpointFromEvent(ev)
	if err != nil {
		return err
	}
	if !ok {
		return ErrInvalidCheckpoint
	}
	// Deleting the old task gives the replacement a new ID and lease token.
	// A completion from a worker claimed before this reset cannot match it.
	if _, err = tx.ExecContext(ctx, `DELETE FROM activity_tasks WHERE run_id=?`, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE workflow_runs SET status='running',current_value=?,next_step=?,updated_at=UTC_TIMESTAMP(6) WHERE id=?`, checkpoint.Value, checkpoint.NextStep, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO activity_tasks (run_id,step_index,state) VALUES (?,?,'pending')`, id, checkpoint.NextStep); err != nil {
		return err
	}
	var sequence int
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM history_events WHERE run_id=?`, id).Scan(&sequence); err != nil {
		return err
	}
	details, err := json.Marshal(map[string]any{"checkpoint_sequence": checkpointSequence, "next_step": checkpoint.NextStep, "previous_status": previousStatus, "previous_step": previousStep, "previous_value": previousValue})
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO history_events (run_id,sequence,type,step_name,details) VALUES (?,?,'WorkflowReset',?,?)`, id, sequence, checkpoint.StepName, details); err != nil {
		return err
	}
	return tx.Commit()
}
