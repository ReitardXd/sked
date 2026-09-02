package scheduler

import (
	"context"
	"log"
	"time"

	"github.com/reitard/sked/internal/job"
	"github.com/reitard/sked/internal/queue"
)

type Scheduler struct {
	repo         *job.Repository
	queue        queue.Queue
	pollInterval time.Duration
	batchSize    int
}

func New(repo *job.Repository, q queue.Queue) *Scheduler {
	return &Scheduler{repo: repo, queue: q, pollInterval: 2 * time.Second, batchSize: 10}
}

// Run polls Postgres for ready jobs and dispatches them onto the queue.
// This is the only place ClaimPendingJobs is called from in Phase 2 —
// workers no longer touch it directly.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.dispatch(ctx)
		}
	}
}

func (s *Scheduler) dispatch(ctx context.Context) {
	jobs, err := s.repo.ClaimPendingJobs(ctx, s.batchSize)
	if err != nil {
		log.Printf("scheduler: claim error: %v", err)
		return
	}
	for _, j := range jobs {
		if err := s.queue.Publish(ctx, j.ID.String()); err != nil {
			log.Printf("scheduler: publish error for job %s: %v", j.ID, err)
			continue
		}
		log.Printf("scheduler: dispatched job %s", j.ID)
	}
}
