package service

import (
	"context"
	"encoding/json"

	"example.com/durable-workflows/backend/internal/engine"
)

// RunEngine is the persistence boundary used by the API service.
type RunEngine interface {
	Start(context.Context, string, json.RawMessage) (string, error)
	ListRuns(context.Context) ([]engine.Run, error)
	GetRun(context.Context, string) (engine.Run, []engine.Event, error)
	ListCheckpoints(context.Context, string) ([]engine.Checkpoint, error)
	Reset(context.Context, string, int) error
}

type Service struct{ engine RunEngine }

func New(e RunEngine) *Service { return &Service{engine: e} }

type RunDetail struct {
	Run         engine.Run          `json:"run"`
	Events      []engine.Event      `json:"events"`
	Checkpoints []engine.Checkpoint `json:"checkpoints"`
}

func (s *Service) Workflows() []engine.Workflow { return engine.Workflows() }

func (s *Service) StartRun(ctx context.Context, name string, input json.RawMessage) (string, error) {
	return s.engine.Start(ctx, name, input)
}

func (s *Service) ListRuns(ctx context.Context) ([]engine.Run, error) {
	return s.engine.ListRuns(ctx)
}

func (s *Service) GetRun(ctx context.Context, id string) (RunDetail, error) {
	run, events, err := s.engine.GetRun(ctx, id)
	if err != nil {
		return RunDetail{}, err
	}
	checkpoints, err := s.engine.ListCheckpoints(ctx, run.ID)
	if err != nil {
		return RunDetail{}, err
	}
	return RunDetail{Run: run, Events: events, Checkpoints: checkpoints}, nil
}

func (s *Service) ResetRun(ctx context.Context, id string, sequence int) (RunDetail, error) {
	if err := s.engine.Reset(ctx, id, sequence); err != nil {
		return RunDetail{}, err
	}
	return s.GetRun(ctx, id)
}
