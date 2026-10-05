package engine

import "context"

// EnsureReplaySchema upgrades databases created by the original single-
// workflow milestone. The initial schema already includes the new column.
func (e *Engine) EnsureReplaySchema(ctx context.Context) error {
	var count int
	if err := e.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='workflow_runs' AND COLUMN_NAME='replay_pending'`).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		_, err := e.DB.ExecContext(ctx, `ALTER TABLE workflow_runs ADD COLUMN replay_pending BOOLEAN NOT NULL DEFAULT FALSE`)
		return err
	}
	return nil
}
