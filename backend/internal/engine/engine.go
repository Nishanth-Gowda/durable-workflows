package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Step struct {
	Name string `json:"name"`
}

type Workflow struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Steps       []Step `json:"steps"`
}

var textWorkflow = Workflow{
	Name: "text-pipeline", Description: "Uppercase text, reverse it, then calculate its SHA-256 digest.",
	Steps: []Step{{Name: "uppercase"}, {Name: "reverse"}, {Name: "sha256"}},
}

func Workflows() []Workflow { return []Workflow{textWorkflow} }

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

type Event struct {
	Sequence   int             `json:"sequence"`
	Type       string          `json:"type"`
	StepName   *string         `json:"step_name"`
	Details    json.RawMessage `json:"details"`
	OccurredAt time.Time       `json:"occurred_at"`
}

type Engine struct{ DB *pgxpool.Pool }

func New(ctx context.Context, databaseURL string) (*Engine, error) {
	db, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(ctx); err != nil {
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
	tx, err := e.DB.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO workflow_runs (id, workflow_name, status, input, current_value)
		VALUES ($1,$2,'running',$3,$4)`, id, workflowName, input, payload.Text)
	if err != nil {
		return "", err
	}
	_, err = tx.Exec(ctx, `INSERT INTO history_events (run_id,sequence,type,details)
		VALUES ($1,1,'WorkflowStarted',$2)`, id, input)
	if err != nil {
		return "", err
	}
	_, err = tx.Exec(ctx, `INSERT INTO activity_tasks (run_id,step_index,state) VALUES ($1,0,'pending')`, id)
	if err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

func scanRun(row pgx.Row) (Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.WorkflowName, &r.Status, &r.Input, &r.CurrentValue, &r.NextStep, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

const runColumns = `id,workflow_name,status,input,current_value,next_step,created_at,updated_at`

func (e *Engine) GetRun(ctx context.Context, id string) (Run, []Event, error) {
	r, err := scanRun(e.DB.QueryRow(ctx, `SELECT `+runColumns+` FROM workflow_runs WHERE id=$1`, id))
	if err != nil {
		return Run{}, nil, err
	}
	rows, err := e.DB.Query(ctx, `SELECT sequence,type,step_name,details,occurred_at FROM history_events WHERE run_id=$1 ORDER BY sequence`, id)
	if err != nil {
		return Run{}, nil, err
	}
	defer rows.Close()
	events := []Event{}
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.Sequence, &event.Type, &event.StepName, &event.Details, &event.OccurredAt); err != nil {
			return Run{}, nil, err
		}
		events = append(events, event)
	}
	return r, events, rows.Err()
}

func (e *Engine) ListRuns(ctx context.Context) ([]Run, error) {
	rows, err := e.DB.Query(ctx, `SELECT `+runColumns+` FROM workflow_runs ORDER BY created_at DESC LIMIT 100`)
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
	Token     string
	Value     string
}

func (e *Engine) claim(ctx context.Context) (*task, error) {
	token, err := randomID()
	if err != nil {
		return nil, err
	}
	var t task
	err = e.DB.QueryRow(ctx, `WITH candidate AS (
		SELECT id FROM activity_tasks
		WHERE (state='pending' AND available_at<=now()) OR (state='leased' AND lease_until<now())
		ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT 1
	)
	UPDATE activity_tasks AS t SET state='leased', attempts=attempts+1,
		lease_token=$1,lease_until=now()+interval '30 seconds'
	FROM candidate WHERE t.id=candidate.id
	RETURNING t.id,t.run_id,t.step_index,t.lease_token`, token).
		Scan(&t.ID, &t.RunID, &t.StepIndex, &t.Token)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	err = e.DB.QueryRow(ctx, `SELECT current_value FROM workflow_runs WHERE id=$1`, t.RunID).Scan(&t.Value)
	return &t, err
}

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

func (e *Engine) complete(ctx context.Context, t *task, output string, activityErr error) error {
	tx, err := e.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var status, currentValue string
	var nextStep int
	err = tx.QueryRow(ctx, `SELECT status,current_value,next_step FROM workflow_runs WHERE id=$1 FOR UPDATE`, t.RunID).
		Scan(&status, &currentValue, &nextStep)
	if err != nil {
		return err
	}
	var attempts int
	err = tx.QueryRow(ctx, `SELECT attempts FROM activity_tasks
		WHERE id=$1 AND state='leased' AND lease_token=$2 AND lease_until>now() FOR UPDATE`, t.ID, t.Token).Scan(&attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	} // Another worker owns the expired lease.
	if err != nil {
		return err
	}
	if status != "running" || nextStep != t.StepIndex {
		return errors.New("run/task state mismatch")
	}
	stepName := textWorkflow.Steps[t.StepIndex].Name
	if activityErr != nil {
		if attempts < 3 {
			_, err = tx.Exec(ctx, `UPDATE activity_tasks SET state='pending',lease_token=NULL,lease_until=NULL,
				available_at=now()+($2 * interval '1 second') WHERE id=$1`, t.ID, attempts*attempts)
			if err != nil {
				return err
			}
		} else {
			_, err = tx.Exec(ctx, `UPDATE activity_tasks SET state='done',lease_token=NULL,lease_until=NULL WHERE id=$1`, t.ID)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE workflow_runs SET status='failed',updated_at=now() WHERE id=$1`, t.RunID)
			if err != nil {
				return err
			}
		}
		kind := "ActivityRetryScheduled"
		if attempts >= 3 {
			kind = "WorkflowFailed"
		}
		details, _ := json.Marshal(map[string]any{"error": activityErr.Error(), "attempt": attempts})
		_, err = tx.Exec(ctx, `INSERT INTO history_events (run_id,sequence,type,step_name,details)
			SELECT $1,COALESCE(MAX(sequence),0)+1,$2,$3,$4 FROM history_events WHERE run_id=$1`, t.RunID, kind, stepName, details)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	_, err = tx.Exec(ctx, `UPDATE activity_tasks SET state='done',lease_token=NULL,lease_until=NULL WHERE id=$1`, t.ID)
	if err != nil {
		return err
	}
	details, _ := json.Marshal(map[string]any{"input": currentValue, "output": output, "attempt": attempts})
	_, err = tx.Exec(ctx, `INSERT INTO history_events (run_id,sequence,type,step_name,details)
		SELECT $1,COALESCE(MAX(sequence),0)+1,'ActivityCompleted',$2,$3 FROM history_events WHERE run_id=$1`, t.RunID, stepName, details)
	if err != nil {
		return err
	}
	if t.StepIndex+1 < len(textWorkflow.Steps) {
		_, err = tx.Exec(ctx, `UPDATE workflow_runs SET current_value=$2,next_step=$3,updated_at=now() WHERE id=$1`, t.RunID, output, t.StepIndex+1)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO activity_tasks (run_id,step_index,state) VALUES ($1,$2,'pending')`, t.RunID, t.StepIndex+1)
		if err != nil {
			return err
		}
	} else {
		_, err = tx.Exec(ctx, `UPDATE workflow_runs SET current_value=$2,next_step=$3,status='completed',updated_at=now() WHERE id=$1`, t.RunID, output, t.StepIndex+1)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO history_events (run_id,sequence,type,details)
			SELECT $1,COALESCE(MAX(sequence),0)+1,'WorkflowCompleted',$2 FROM history_events WHERE run_id=$1`, t.RunID, details)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

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
