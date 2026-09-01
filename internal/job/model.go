package job

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusDispatched Status = "dispatched"
	StatusRunning    Status = "running"
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
	StatusDead       Status = "dead" // exhausted max_attempts, dead-lettered
)

type Job struct {
	ID              uuid.UUID       `db:"id"`
	Payload         json.RawMessage `db:"payload"`
	Status          Status          `db:"status"`
	Priority        int16           `db:"priority"`
	RunAt           time.Time       `db:"run_at"`
	Attempts        int             `db:"attempts"`
	MaxAttempts     int             `db:"max_attempts"`
	IdempotencyKey  *string         `db:"idempotency_key"`
	LastError       *string         `db:"last_error"`
	Result          json.RawMessage `db:"result"`
	CreatedAt       time.Time       `db:"created_at"`
	UpdatedAt       time.Time       `db:"updated_at"`
}

// NewJob builds a Job ready for insertion. Leave idempotencyKey nil if the
// caller doesn't need dedup semantics for this job.
func NewJob(payload json.RawMessage, priority int16, runAt time.Time, maxAttempts int, idempotencyKey *string) *Job {
	return &Job{
		ID:             uuid.New(),
		Payload:        payload,
		Status:         StatusPending,
		Priority:       priority,
		RunAt:          runAt,
		MaxAttempts:    maxAttempts,
		IdempotencyKey: idempotencyKey,
	}
}

func (j *Job) IsExhausted() bool {
	return j.Attempts >= j.MaxAttempts
}
