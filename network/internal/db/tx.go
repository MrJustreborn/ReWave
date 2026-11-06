package db

import (
	"context"
	"database/sql"
	"fmt"
)

// WithTransaction runs fn inside a transaction on d: commit on success,
// rollback on error (and a combined error if the rollback fails too).
func WithTransaction(ctx context.Context, d *sql.DB, fn func(ctx context.Context, tx *sql.Tx) error) error {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("%w (rollback failed: %v)", err, rollbackErr)
		}
		return err
	}
	return tx.Commit()
}
