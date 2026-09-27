export type Workflow = {
  name: string;
  description: string;
  steps: { name: string }[];
};

export type Run = {
  id: string;
  workflow_name: string;
  status: string;
  created_at: string;
  updated_at: string;
  input?: Record<string, unknown>;
};

export type Event = {
  sequence: number;
  type: string;
  step_name?: string;
  occurred_at: string;
  details?: unknown;
};

export type Checkpoint = {
  sequence: number;
  next_step: number;
  step_name: string | null;
  value: string;
};

export type RunDetail = { run: Run; events: Event[]; checkpoints: Checkpoint[] };
