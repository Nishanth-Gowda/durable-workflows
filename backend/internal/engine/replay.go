package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// A workflow function describes decisions in Go. Activity calls are the only
// places where it can block. Replaying returns recorded results at those calls.
var errWorkflowBlocked = errors.New("workflow waiting for activity")

type activityRecord struct {
	name      string
	input     string
	output    string
	completed bool
}

type replayContext struct {
	records []activityRecord
	cursor  int
	pending *activityRecord
}

func (c *replayContext) Activity(name, input string) (string, error) {
	index := c.cursor
	c.cursor++
	if index < len(c.records) {
		record := c.records[index]
		if record.name != name || record.input != input {
			return "", fmt.Errorf("non-deterministic workflow at activity %d: history has %q(%q), code requested %q(%q)", index, record.name, record.input, name, input)
		}
		if record.completed {
			return record.output, nil
		}
		c.pending = &record
		return "", errWorkflowBlocked
	}
	c.pending = &activityRecord{name: name, input: input}
	return "", errWorkflowBlocked
}

// This is workflow-authored control flow: the result of the first activity
// chooses the next one. The engine never stores this sequence as a step list.
func branchingTextWorkflow(ctx *replayContext, input string) (string, error) {
	upper, err := ctx.Activity("uppercase", input)
	if err != nil {
		return "", err
	}
	if len([]rune(upper)) > 12 {
		return ctx.Activity("sha256", upper)
	}
	return ctx.Activity("reverse", upper)
}

func replayRecords(events []Event) ([]activityRecord, error) {
	records := []activityRecord{}
	for _, ev := range events {
		switch ev.Type {
		case "ActivityScheduled":
			if ev.StepName == nil {
				return nil, errors.New("scheduled activity has no name")
			}
			var details struct {
				Input string `json:"input"`
			}
			if err := json.Unmarshal(ev.Details, &details); err != nil {
				return nil, err
			}
			records = append(records, activityRecord{name: *ev.StepName, input: details.Input})
		case "ActivityCompleted":
			if len(records) == 0 || records[len(records)-1].completed || ev.StepName == nil || records[len(records)-1].name != *ev.StepName {
				return nil, errors.New("activity completion does not match its schedule")
			}
			var details struct {
				Output string `json:"output"`
			}
			if err := json.Unmarshal(ev.Details, &details); err != nil {
				return nil, err
			}
			records[len(records)-1].output = details.Output
			records[len(records)-1].completed = true
		}
	}
	return records, nil
}

func replayBranch(events []Event, input string) (string, *activityRecord, int, error) {
	records, err := replayRecords(events)
	if err != nil {
		return "", nil, 0, err
	}
	ctx := &replayContext{records: records}
	result, err := branchingTextWorkflow(ctx, input)
	if errors.Is(err, errWorkflowBlocked) {
		return "", ctx.pending, ctx.cursor - 1, nil
	}
	if err != nil {
		return "", nil, 0, err
	}
	if ctx.cursor != len(records) {
		return "", nil, 0, errors.New("non-deterministic workflow: history has extra activity commands")
	}
	return result, nil, ctx.cursor, nil
}

func (e *Engine) startBranch(ctx context.Context, name string, input json.RawMessage, text string) (string, error) {
	id, err := randomID()
	if err != nil {
		return "", err
	}
	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO workflow_runs (id,workflow_name,status,input,current_value,replay_pending) VALUES (?,?,'running',?,?,TRUE)`, id, name, input, text); err != nil {
		return "", err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO history_events (run_id,sequence,type,details) VALUES (?,1,'WorkflowStarted',?)`, id, input); err != nil {
		return "", err
	}
	return id, tx.Commit()
}

// processBranchWorkflowTask is the durable wakeup. Only pure workflow code
// runs here, and schedule/completion is committed with clearing the wakeup.
func (e *Engine) processBranchWorkflowTask(ctx context.Context) (bool, error) {
	tx, err := e.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var runID string
	var input json.RawMessage
	err = tx.QueryRowContext(ctx, `SELECT id,input FROM workflow_runs WHERE workflow_name=? AND status='running' AND replay_pending=TRUE ORDER BY created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED`, branchingWorkflow.Name).Scan(&runID, &input)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var payload struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(input, &payload); err != nil {
		return false, err
	}
	events, err := loadReplayEvents(ctx, tx, runID)
	if err != nil {
		return false, err
	}
	result, pending, index, err := replayBranch(events, payload.Text)
	if err != nil {
		return false, err
	}
	sequence := events[len(events)-1].Sequence + 1
	if pending != nil {
		// A scheduled but unfinished activity is already durable. Normally a
		// wakeup follows completion, so the pending command is new.
		if index < countScheduled(events) {
			return false, errors.New("workflow wakeup has an unfinished activity")
		}
		details, _ := json.Marshal(map[string]string{"input": pending.input})
		if _, err = tx.ExecContext(ctx, `INSERT INTO history_events (run_id,sequence,type,step_name,details) VALUES (?,?,'ActivityScheduled',?,?)`, runID, sequence, pending.name, details); err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO activity_tasks (run_id,step_index,state) VALUES (?,?,'pending')`, runID, index); err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE workflow_runs SET current_value=?,next_step=?,replay_pending=FALSE,updated_at=UTC_TIMESTAMP(6) WHERE id=?`, pending.input, index, runID); err != nil {
			return false, err
		}
	} else {
		details, _ := json.Marshal(map[string]string{"output": result})
		if _, err = tx.ExecContext(ctx, `INSERT INTO history_events (run_id,sequence,type,details) VALUES (?,?,'WorkflowCompleted',?)`, runID, sequence, details); err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE workflow_runs SET current_value=?,next_step=?,status='completed',replay_pending=FALSE,updated_at=UTC_TIMESTAMP(6) WHERE id=?`, result, index, runID); err != nil {
			return false, err
		}
	}
	return true, tx.Commit()
}

func countScheduled(events []Event) int {
	count := 0
	for _, event := range events {
		if event.Type == "ActivityScheduled" {
			count++
		}
	}
	return count
}

func loadReplayEvents(ctx context.Context, tx *sql.Tx, runID string) ([]Event, error) {
	rows, err := tx.QueryContext(ctx, `SELECT sequence,type,step_name,details FROM history_events WHERE run_id=? ORDER BY sequence`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []Event{}
	for rows.Next() {
		var ev Event
		var name sql.NullString
		if err := rows.Scan(&ev.Sequence, &ev.Type, &name, &ev.Details); err != nil {
			return nil, err
		}
		if name.Valid {
			ev.StepName = &name.String
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

var branchActivities = map[string]func(string) (string, error){
	"uppercase": func(value string) (string, error) { return strings.ToUpper(value), nil },
	"reverse": func(value string) (string, error) {
		runes := []rune(value)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), nil
	},
	"sha256": func(value string) (string, error) {
		digest := sha256.Sum256([]byte(value))
		return hex.EncodeToString(digest[:]), nil
	},
}

func executeBranch(t *task) (string, error) {
	handler, ok := branchActivities[t.ActivityName]
	if !ok {
		return "", fmt.Errorf("unknown activity %q", t.ActivityName)
	}
	return handler(t.Value)
}

// A completion records the result and a replay wakeup in one transaction.
// A crash after this commit cannot lose the next workflow decision.
func (e *Engine) completeBranch(ctx context.Context, t *task, output string, activityErr error) error {
	tx, err := e.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status string
	var nextStep int
	if err = tx.QueryRowContext(ctx, `SELECT status,next_step FROM workflow_runs WHERE id=? FOR UPDATE`, t.RunID).Scan(&status, &nextStep); err != nil {
		return err
	}
	var attempts int
	err = tx.QueryRowContext(ctx, `SELECT attempts FROM activity_tasks WHERE id=? AND state='leased' AND lease_token=? AND lease_until>UTC_TIMESTAMP(6) FOR UPDATE`, t.ID, t.Token).Scan(&attempts)
	if errors.Is(err, sql.ErrNoRows) {
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
	if activityErr != nil {
		kind := "ActivityRetryScheduled"
		if attempts < 3 {
			_, err = tx.ExecContext(ctx, `UPDATE activity_tasks SET state='pending',lease_token=NULL,lease_until=NULL,available_at=TIMESTAMPADD(SECOND,?,UTC_TIMESTAMP(6)) WHERE id=?`, attempts*attempts, t.ID)
		} else {
			kind = "WorkflowFailed"
			_, err = tx.ExecContext(ctx, `UPDATE activity_tasks SET state='done',lease_token=NULL,lease_until=NULL WHERE id=?`, t.ID)
			if err == nil {
				_, err = tx.ExecContext(ctx, `UPDATE workflow_runs SET status='failed',updated_at=UTC_TIMESTAMP(6) WHERE id=?`, t.RunID)
			}
		}
		if err != nil {
			return err
		}
		details, _ := json.Marshal(map[string]any{"error": activityErr.Error(), "attempt": attempts})
		if _, err = tx.ExecContext(ctx, `INSERT INTO history_events (run_id,sequence,type,step_name,details) VALUES (?,?,?,?,?)`, t.RunID, sequence, kind, t.ActivityName, details); err != nil {
			return err
		}
		return tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, `UPDATE activity_tasks SET state='done',lease_token=NULL,lease_until=NULL WHERE id=?`, t.ID); err != nil {
		return err
	}
	details, _ := json.Marshal(map[string]any{"input": t.Value, "output": output, "attempt": attempts})
	if _, err = tx.ExecContext(ctx, `INSERT INTO history_events (run_id,sequence,type,step_name,details) VALUES (?,?,'ActivityCompleted',?,?)`, t.RunID, sequence, t.ActivityName, details); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE workflow_runs SET replay_pending=TRUE,updated_at=UTC_TIMESTAMP(6) WHERE id=?`, t.RunID); err != nil {
		return err
	}
	return tx.Commit()
}
