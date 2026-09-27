-- Stores the current durable state of each workflow execution.
CREATE TABLE IF NOT EXISTS workflow_runs (
  -- The run ID is the stable identifier referenced by child records.
  id CHAR(32) PRIMARY KEY,
  workflow_name VARCHAR(128) NOT NULL,
  -- Restrict the lifecycle to states understood by the engine.
  status VARCHAR(16) NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
  -- Keep the submitted JSON for inspection; workers use current_value.
  input JSON NOT NULL,
  -- Checkpoint passed to the next step, then the final result on completion.
  current_value TEXT NOT NULL,
  next_step INT NOT NULL DEFAULT 0,
  created_at DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
  updated_at DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6))
) ENGINE=InnoDB;

-- Stores the append-only history for each workflow run.
CREATE TABLE IF NOT EXISTS history_events (
  run_id CHAR(32) NOT NULL,
  sequence INT NOT NULL,
  type VARCHAR(64) NOT NULL,
  step_name VARCHAR(128),
  details JSON NOT NULL,
  occurred_at DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
  -- Sequence numbers are unique within a run and preserve event order.
  PRIMARY KEY (run_id, sequence),
  -- A history event cannot outlive the workflow run it belongs to.
  CONSTRAINT history_run_fk FOREIGN KEY (run_id) REFERENCES workflow_runs(id)
) ENGINE=InnoDB;

-- Stores activity work items, including their current lease and retry timing.
CREATE TABLE IF NOT EXISTS activity_tasks (
  -- Auto-incremented task ID also provides a stable claim ordering tie-breaker.
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  run_id CHAR(32) NOT NULL,
  step_index INT NOT NULL,
  -- A task is either waiting, currently leased, or finished.
  state VARCHAR(16) NOT NULL CHECK (state IN ('pending', 'leased', 'done')),
  attempts INT NOT NULL DEFAULT 0,
  -- A new token fences an old worker after this lease is reclaimed.
  lease_token CHAR(32),
  lease_until DATETIME(6),
  -- Retry backoff keeps a pending task unavailable until this time.
  available_at DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
  -- A workflow step has at most one durable task record.
  UNIQUE KEY task_run_step (run_id, step_index),
  -- Supports workers looking for claimable tasks and expired leases.
  KEY task_claim (state, available_at, lease_until, id),
  -- A task cannot exist without its workflow run.
  CONSTRAINT task_run_fk FOREIGN KEY (run_id) REFERENCES workflow_runs(id)
) ENGINE=InnoDB;
