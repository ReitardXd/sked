package worker

import (
	"context"
	"log"
	"time"

	"github.com/reitard/sked/internal/job"
	"github.com/reitard/sked/internal/queue"
)

type Worker struct {
	repo  *job.Repository
	queue queue.Queue
	name  string
}

func New(repo *job.Repository, q queue.Queue, name string) *Worker {
	return &Worker{repo: repo, queue: q, name: name}
}

func (w *Worker) Run(ctx context.Context) {
	messages, err := w.queue.Consume(ctx, w.name)
	if err != nil {
		log.Fatalf("worker: consume setup failed: %v", err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			w.handle(ctx, msg)
		}
	}
}

func (w *Worker) handle(ctx context.Context, msg queue.Message) {
	jobID, err := uuidFromString(msg.JobID)
	if err != nil {
		log.Printf("worker: bad job id %q: %v", msg.JobID, err)
		msg.Ack(ctx) // drop malformed message, nothing we can do with it
		return
	}

	j, err := w.repo.GetJob(ctx, jobID)
	if err != nil {
		log.Printf("worker: fetch job %s: %v", jobID, err)
		return // don't ack — let it be redelivered
	}

	if err := w.repo.MarkRunning(ctx, j.ID); err != nil {
		log.Printf("worker: mark running %s: %v", j.ID, err)
		return
	}

	result, execErr := Execute(j)
	if execErr != nil {
		nextRunAt := time.Now().Add(backoff(j.Attempts))
		if err := w.repo.MarkFailed(ctx, j.ID, execErr.Error(), nextRunAt); err != nil {
			log.Printf("worker: mark failed %s: %v", j.ID, err)
		}
		log.Printf("worker: job %s failed: %v (attempt %d/%d)", j.ID, execErr, j.Attempts+1, j.MaxAttempts)
		msg.Ack(ctx) // scheduler will re-dispatch on next poll since status=pending again
		return
	}

	if err := w.repo.MarkSucceeded(ctx, j.ID, result); err != nil {
		log.Printf("worker: mark succeeded %s: %v", j.ID, err)
		return
	}
	log.Printf("worker: job %s succeeded", j.ID)
	msg.Ack(ctx)
}

func backoff(attempts int) time.Duration {
	d := time.Duration(1<<attempts) * time.Second
	if d > 5*time.Minute {
		return 5 * time.Minute
	}
	return d
}
