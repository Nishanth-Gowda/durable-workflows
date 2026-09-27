package handlers

import (
	"context"
	"encoding/json"

	"example.com/durable-workflows/backend/internal/engine"
	"example.com/durable-workflows/backend/internal/service"
)

type RunService interface {
	Workflows() []engine.Workflow
	StartRun(context.Context, string, json.RawMessage) (string, error)
	ListRuns(context.Context) ([]engine.Run, error)
	GetRun(context.Context, string) (service.RunDetail, error)
	ResetRun(context.Context, string, int) (service.RunDetail, error)
}

type Handlers struct{ runs RunService }

func New(runs RunService) *Handlers { return &Handlers{runs: runs} }
