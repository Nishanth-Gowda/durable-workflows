package engine

import (
	"encoding/json"
	"time"
)

type Step struct {
	Name string `json:"name"`
}

// Workflow describes the activities shown by the API. Branching workflow
// decisions live in Go code, so this list does not prescribe execution order.
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

var branchingWorkflow = Workflow{
	Name: "branching-text", Description: "Uppercase text, then choose reverse or SHA-256 in Go workflow code.",
	Steps: []Step{{Name: "uppercase"}, {Name: "reverse"}, {Name: "sha256"}},
}

func Workflows() []Workflow { return []Workflow{textWorkflow, branchingWorkflow} }

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

// Checkpoint is an immutable value saved before an activity. Sequence refers
// to the history event that produced the value, so old branches stay usable.
type Checkpoint struct {
	Sequence int     `json:"sequence"`
	NextStep int     `json:"next_step"`
	StepName *string `json:"step_name"`
	Value    string  `json:"value"`
}

type task struct {
	ID           int64
	RunID        string
	StepIndex    int
	WorkflowName string
	ActivityName string
	Token        string // Fences a worker after its lease expires or is replaced.
	Value        string // Checkpoint read when this task was claimed.
}

type scanner interface{ Scan(...any) error }

func scanRun(row scanner) (Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.WorkflowName, &r.Status, &r.Input, &r.CurrentValue, &r.NextStep, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

const runColumns = `id,workflow_name,status,input,current_value,next_step,created_at,updated_at`
