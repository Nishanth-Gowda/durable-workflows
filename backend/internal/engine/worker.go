package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

// claim leases one ready task. SKIP LOCKED lets other worker processes claim
// different tasks without waiting for this transaction's row lock.
func (e *Engine) claim(ctx context.Context) (*task, error) {
	token, err := randomID()
	if err != nil {
		return nil, err
	}
	tx, err := e.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var t task
	err = tx.QueryRowContext(ctx, `SELECT id,run_id,step_index FROM activity_tasks
		WHERE (state='pending' AND available_at<=UTC_TIMESTAMP(6)) OR (state='leased' AND lease_until<UTC_TIMESTAMP(6))
		ORDER BY available_at,id LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&t.ID, &t.RunID, &t.StepIndex)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	t.Token = token
	// Expired leases are reclaimable; every claim gets a new token and attempt.
	if _, err = tx.ExecContext(ctx, `UPDATE activity_tasks SET state='leased',attempts=attempts+1,lease_token=?,lease_until=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 30 SECOND) WHERE id=?`, token, t.ID); err != nil {
		return nil, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT current_value FROM workflow_runs WHERE id=?`, t.RunID).Scan(&t.Value); err != nil {
		return nil, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT workflow_name FROM workflow_runs WHERE id=?`, t.RunID).Scan(&t.WorkflowName); err != nil {
		return nil, err
	}
	if t.WorkflowName == branchingWorkflow.Name {
		var details json.RawMessage
		if err = tx.QueryRowContext(ctx, `SELECT step_name,details FROM history_events WHERE run_id=? AND type='ActivityScheduled' ORDER BY sequence DESC LIMIT 1`, t.RunID).Scan(&t.ActivityName, &details); err != nil {
			return nil, err
		}
		var scheduled struct {
			Input string `json:"input"`
		}
		if err = json.Unmarshal(details, &scheduled); err != nil {
			return nil, err
		}
		t.Value = scheduled.Input
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &t, nil
}

// execute applies the fixed workflow definition to the saved checkpoint.
func execute(stepIndex int, value string) (string, error) {
	switch stepIndex {
	case 0:
		return strings.ToUpper(value), nil
	case 1:
		runes := []rune(value)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), nil
	case 2:
		digest := sha256.Sum256([]byte(value))
		return hex.EncodeToString(digest[:]), nil
	default:
		return "", fmt.Errorf("unknown step %d", stepIndex)
	}
}

// complete commits one activity transition. Locking the run serializes its
// history sequence and checkpoint update with creation of the next task.
func (e *Engine) complete(ctx context.Context, t *task, output string, activityErr error) error {
	if t.WorkflowName == branchingWorkflow.Name {
		return e.completeBranch(ctx, t, output, activityErr)
	}
	tx, err := e.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status, currentValue string
	var nextStep int
	if err = tx.QueryRowContext(ctx, `SELECT status,current_value,next_step FROM workflow_runs WHERE id=? FOR UPDATE`, t.RunID).Scan(&status, &currentValue, &nextStep); err != nil {
		return err
	}
	var attempts int
	err = tx.QueryRowContext(ctx, `SELECT attempts FROM activity_tasks WHERE id=? AND state='leased' AND lease_token=? AND lease_until>UTC_TIMESTAMP(6) FOR UPDATE`, t.ID, t.Token).Scan(&attempts)
	if errors.Is(err, sql.ErrNoRows) {
		// This worker lost its lease; a replacement may already own the task.
		return nil
	}
	if err != nil {
		return err
	}
	if status != "running" || nextStep != t.StepIndex {
		return errors.New("run/task state mismatch")
	}
	var sequence int
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM history_events WHERE run_id=?`, t.RunID).Scan(&sequence); err != nil {
		return err
	}
	stepName := textWorkflow.Steps[t.StepIndex].Name
	if activityErr != nil {
		// Back off after the first two failures, then fail the run on attempt 3.
		if attempts < 3 {
			_, err = tx.ExecContext(ctx, `UPDATE activity_tasks SET state='pending',lease_token=NULL,lease_until=NULL,available_at=TIMESTAMPADD(SECOND,?,UTC_TIMESTAMP(6)) WHERE id=?`, attempts*attempts, t.ID)
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE activity_tasks SET state='done',lease_token=NULL,lease_until=NULL WHERE id=?`, t.ID)
			if err == nil {
				_, err = tx.ExecContext(ctx, `UPDATE workflow_runs SET status='failed',updated_at=UTC_TIMESTAMP(6) WHERE id=?`, t.RunID)
			}
		}
		if err != nil {
			return err
		}
		kind := "ActivityRetryScheduled"
		if attempts >= 3 {
			kind = "WorkflowFailed"
		}
		details, _ := json.Marshal(map[string]any{"error": activityErr.Error(), "attempt": attempts})
		if _, err = tx.ExecContext(ctx, `INSERT INTO history_events (run_id,sequence,type,step_name,details) VALUES (?,?,?,?,?)`, t.RunID, sequence, kind, stepName, details); err != nil {
			return err
		}
		return tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, `UPDATE activity_tasks SET state='done',lease_token=NULL,lease_until=NULL WHERE id=?`, t.ID); err != nil {
		return err
	}
	details, _ := json.Marshal(map[string]any{"input": currentValue, "output": output, "attempt": attempts})
	if _, err = tx.ExecContext(ctx, `INSERT INTO history_events (run_id,sequence,type,step_name,details) VALUES (?,?,'ActivityCompleted',?,?)`, t.RunID, sequence, stepName, details); err != nil {
		return err
	}
	// The next task and its input checkpoint become visible together.
	if t.StepIndex+1 < len(textWorkflow.Steps) {
		if _, err = tx.ExecContext(ctx, `UPDATE workflow_runs SET current_value=?,next_step=?,updated_at=UTC_TIMESTAMP(6) WHERE id=?`, output, t.StepIndex+1, t.RunID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO activity_tasks (run_id,step_index,state) VALUES (?,?,'pending')`, t.RunID, t.StepIndex+1); err != nil {
			return err
		}
	} else {
		if _, err = tx.ExecContext(ctx, `UPDATE workflow_runs SET current_value=?,next_step=?,status='completed',updated_at=UTC_TIMESTAMP(6) WHERE id=?`, output, t.StepIndex+1, t.RunID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO history_events (run_id,sequence,type,details) VALUES (?,?,'WorkflowCompleted',?)`, t.RunID, sequence+1, details); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Work polls for tasks and executes them serially within this process.
// Additional worker processes can claim other tasks concurrently.
func (e *Engine) Work(ctx context.Context, activityDelay time.Duration) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		if processed, err := e.processBranchWorkflowTask(ctx); err != nil {
			return err
		} else if processed {
			continue
		}
		t, err := e.claim(ctx)
		if err != nil {
			return err
		}
		if t == nil {
			continue
		}
		log.Printf("claimed run=%s step=%d task=%d", t.RunID, t.StepIndex, t.ID)
		if activityDelay > 0 {
			// This optional pause makes lease recovery observable in the demo.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(activityDelay):
			}
		}
		var output string
		var activityErr error
		if t.WorkflowName == branchingWorkflow.Name {
			output, activityErr = executeBranch(t)
		} else {
			output, activityErr = execute(t.StepIndex, t.Value)
		}
		if err := e.complete(ctx, t, output, activityErr); err != nil {
			return err
		}
	}
}
