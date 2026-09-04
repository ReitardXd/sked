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
	elector      *LeaderElector
	pollInterval time.Duration
	batchSize    int
}

func New(repo *job.Repository, q queue.Queue, elector *LeaderElector) *Scheduler {
	return &Scheduler{repo: repo, queue: q, elector: elector, pollInterval: 2 * time.Second, batchSize: 10}
}

func (s *Scheduler) Run(ctx context.Context) {
	go s.elector.Run(ctx)

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.elector.IsLeader() {
				continue // standby: don't claim/dispatch unless leader
			}
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
