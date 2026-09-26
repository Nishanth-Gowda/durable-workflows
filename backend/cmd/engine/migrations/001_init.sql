CREATE TABLE IF NOT EXISTS workflow_runs (
  id text PRIMARY KEY,
  workflow_name text NOT NULL,
  status text NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
  input jsonb NOT NULL,
  current_value text NOT NULL,
  next_step integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS history_events (
  run_id text NOT NULL REFERENCES workflow_runs(id),
  sequence integer NOT NULL,
  type text NOT NULL,
  step_name text,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  occurred_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, sequence)
);

CREATE TABLE IF NOT EXISTS activity_tasks (
  id bigserial PRIMARY KEY,
  run_id text NOT NULL REFERENCES workflow_runs(id),
  step_index integer NOT NULL,
  state text NOT NULL CHECK (state IN ('pending', 'leased', 'done')),
  attempts integer NOT NULL DEFAULT 0,
  lease_token text,
  lease_until timestamptz,
  available_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (run_id, step_index)
);

CREATE INDEX IF NOT EXISTS activity_tasks_claim_idx
  ON activity_tasks (available_at, id) WHERE state <> 'done';
