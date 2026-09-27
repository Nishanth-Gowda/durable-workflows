package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Step struct {
	Name string `json:"name"`
}

// Workflow describes the steps exposed by the API. Execution is currently
// defined by the matching step indexes in execute.
type Workflow struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Steps       []Step `json:"steps"`
}

// The engine currently supports one fixed, sequential workflow.
var textWorkflow = Workflow{
	Name: "text-pipeline", Description: "Uppercase text, reverse it, then calculate its SHA-256 digest.",
	Steps: []Step{{Name: "uppercase"}, {Name: "reverse"}, {Name: "sha256"}},
}

func Workflows() []Workflow { return []Workflow{textWorkflow} }

// Run is the durable checkpoint for one workflow execution. CurrentValue is
// the input to NextStep, or the final result once Status is completed.
type Run struct {
	ID           string          `json:"id"`
	WorkflowName string          `json:"workflow_name"`
	Status       string          `json:"status"`
	Input        json.RawMessage `json:"input"`
	CurrentValue string          `json:"current_value"`
	NextStep     int             `json:"next_step"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// Event records an ordered transition in a run's history.
type Event struct {
	Sequence   int             `json:"sequence"`
	Type       string          `json:"type"`
	StepName   *string         `json:"step_name"`
	Details    json.RawMessage `json:"details"`
	OccurredAt time.Time       `json:"occurred_at"`
}

// Engine shares a connection pool between API requests and workers.
type Engine struct{ DB *sql.DB }

func New(ctx context.Context, dsn string) (*Engine, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(3 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &Engine{DB: db}, nil
}
func (e *Engine) Close() { e.DB.Close() }
func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
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

type scanner interface{ Scan(...any) error }

func scanRun(row scanner) (Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.WorkflowName, &r.Status, &r.Input, &r.CurrentValue, &r.NextStep, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

const runColumns = `id,workflow_name,status,input,current_value,next_step,created_at,updated_at`

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

type task struct {
	ID        int64
	RunID     string
	StepIndex int
	Token     string // Fences a worker after its lease expires or is replaced.
	Value     string // Checkpoint read when this task was claimed.
}

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
		output, activityErr := execute(t.StepIndex, t.Value)
		if err := e.complete(ctx, t, output, activityErr); err != nil {
			return err
		}
	}
}
