package worker

import (
	"context"
	"log"
	"time"

	"github.com/reitard/sked/internal/job"
)

type Worker struct {
	repo         *job.Repository
	pollInterval time.Duration
	batchSize    int
}

func New(repo *job.Repository) *Worker {
	return &Worker{repo: repo, pollInterval: 2 * time.Second, batchSize: 5}
}

// Run polls for pending jobs directly (Phase 1 — no queue yet).
// In Phase 2 this gets replaced by consuming from Redis/RabbitMQ.
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.pollAndExecute(ctx)
		}
	}
}

func (w *Worker) pollAndExecute(ctx context.Context) {
	jobs, err := w.repo.ClaimPendingJobs(ctx, w.batchSize)
	if err != nil {
		log.Printf("claim error: %v", err)
		return
	}
	for _, j := range jobs {
		w.execute(ctx, j)
	}
}

func (w *Worker) execute(ctx context.Context, j *job.Job) {
	if err := w.repo.MarkRunning(ctx, j.ID); err != nil {
		log.Printf("mark running error for %s: %v", j.ID, err)
		return
	}

	result, err := Execute(j)
	if err != nil {
		nextRunAt := time.Now().Add(backoff(j.Attempts))
		if markErr := w.repo.MarkFailed(ctx, j.ID, err.Error(), nextRunAt); markErr != nil {
			log.Printf("mark failed error for %s: %v", j.ID, markErr)
		}
		log.Printf("job %s failed: %v (attempt %d/%d)", j.ID, err, j.Attempts+1, j.MaxAttempts)
		return
	}

	if err := w.repo.MarkSucceeded(ctx, j.ID, result); err != nil {
		log.Printf("mark succeeded error for %s: %v", j.ID, err)
		return
	}
	log.Printf("job %s succeeded", j.ID)
}

// backoff is a simple exponential backoff, capped at 5 minutes.
func backoff(attempts int) time.Duration {
	d := time.Duration(1<<attempts) * time.Second
	if d > 5*time.Minute {
		return 5 * time.Minute
	}
	return d
}
