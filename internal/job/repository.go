package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("job: not found")

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) CreateJob(ctx context.Context, j *Job) error {
	const q = `
		INSERT INTO jobs (id, payload, status, priority, run_at, max_attempts, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err := r.pool.Exec(ctx, q,
		j.ID, j.Payload, j.Status, j.Priority, j.RunAt, j.MaxAttempts, j.IdempotencyKey,
	)
	if err != nil {
		return fmt.Errorf("create job: %w", err)
	}
	return nil
}

func (r *Repository) GetJob(ctx context.Context, id uuid.UUID) (*Job, error) {
	const q = `
		SELECT id, payload, status, priority, run_at, attempts, max_attempts,
		       idempotency_key, last_error, result, created_at, updated_at
		FROM jobs WHERE id = $1
	`
	row := r.pool.QueryRow(ctx, q, id)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

// ClaimPendingJobs is the core concurrency-safe dequeue. It selects up to
// `limit` pending jobs that are due to run, locks them so no other scheduler
// replica can grab the same rows (SKIP LOCKED means concurrent callers never
// block on each other, they just get different rows), and atomically flips
// them to 'dispatched' in the same transaction. Safe to call from multiple
// scheduler instances concurrently.
func (r *Repository) ClaimPendingJobs(ctx context.Context, limit int) ([]*Job, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("claim jobs: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if committed

	const selectQ = `
		SELECT id, payload, status, priority, run_at, attempts, max_attempts,
		       idempotency_key, last_error, result, created_at, updated_at
		FROM jobs
		WHERE status = 'pending' AND run_at <= now()
		ORDER BY priority DESC, run_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`
	rows, err := tx.Query(ctx, selectQ, limit)
	if err != nil {
		return nil, fmt.Errorf("claim jobs: select: %w", err)
	}

	var jobs []*Job
	var ids []uuid.UUID
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("claim jobs: scan: %w", err)
		}
		jobs = append(jobs, j)
		ids = append(ids, j.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim jobs: rows: %w", err)
	}
	if len(jobs) == 0 {
		return nil, tx.Commit(ctx)
	}

	const updateQ = `
		UPDATE jobs SET status = 'dispatched', updated_at = now()
		WHERE id = ANY($1)
	`
	if _, err := tx.Exec(ctx, updateQ, ids); err != nil {
		return nil, fmt.Errorf("claim jobs: update: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("claim jobs: commit: %w", err)
	}
	for _, j := range jobs {
		j.Status = StatusDispatched
	}
	return jobs, nil
}

func (r *Repository) MarkRunning(ctx context.Context, id uuid.UUID) error {
	const q = `UPDATE jobs SET status = 'running', attempts = attempts + 1, updated_at = now() WHERE id = $1`
	_, err := r.pool.Exec(ctx, q, id)
	return err
}

func (r *Repository) MarkSucceeded(ctx context.Context, id uuid.UUID, result json.RawMessage) error {
	const q = `UPDATE jobs SET status = 'succeeded', result = $2, updated_at = now() WHERE id = $1`
	_, err := r.pool.Exec(ctx, q, id, result)
	return err
}

// MarkFailed records the error. If the job has exhausted max_attempts it goes
// to 'dead' (dead-letter); otherwise it's requeued as 'pending' with a
// backoff-adjusted run_at for retry.
func (r *Repository) MarkFailed(ctx context.Context, id uuid.UUID, errMsg string, nextRunAt time.Time) error {
	const q = `
		UPDATE jobs
		SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
		    last_error = $2,
		    run_at = $3,
		    updated_at = now()
		WHERE id = $1
	`
	_, err := r.pool.Exec(ctx, q, id, errMsg, nextRunAt)
	return err
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(row rowScanner) (*Job, error) {
	var j Job
	err := row.Scan(
		&j.ID, &j.Payload, &j.Status, &j.Priority, &j.RunAt, &j.Attempts, &j.MaxAttempts,
		&j.IdempotencyKey, &j.LastError, &j.Result, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &j, nil
}
