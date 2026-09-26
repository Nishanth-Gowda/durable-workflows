CREATE TABLE IF NOT EXISTS workflow_runs (
  id CHAR(32) PRIMARY KEY,
  workflow_name VARCHAR(128) NOT NULL,
  status VARCHAR(16) NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
  input JSON NOT NULL,
  current_value TEXT NOT NULL,
  next_step INT NOT NULL DEFAULT 0,
  created_at DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
  updated_at DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS history_events (
  run_id CHAR(32) NOT NULL,
  sequence INT NOT NULL,
  type VARCHAR(64) NOT NULL,
  step_name VARCHAR(128),
  details JSON NOT NULL,
  occurred_at DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
  PRIMARY KEY (run_id, sequence),
  CONSTRAINT history_run_fk FOREIGN KEY (run_id) REFERENCES workflow_runs(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS activity_tasks (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  run_id CHAR(32) NOT NULL,
  step_index INT NOT NULL,
  state VARCHAR(16) NOT NULL CHECK (state IN ('pending', 'leased', 'done')),
  attempts INT NOT NULL DEFAULT 0,
  lease_token CHAR(32),
  lease_until DATETIME(6),
  available_at DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
  UNIQUE KEY task_run_step (run_id, step_index),
  KEY task_claim (state, available_at, lease_until, id),
  CONSTRAINT task_run_fk FOREIGN KEY (run_id) REFERENCES workflow_runs(id)
) ENGINE=InnoDB;
