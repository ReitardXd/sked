package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/reitard/sked/internal/job"

)

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/djs?sslmode=disable")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	repo := job.NewRepository(pool)

	// 1. Creating few jobs 
	for i := 0; i < 3; i++ {
		payload, _ := json.Marshal(map[string]any{"task": fmt.Sprintf("job-%d", i)})
		j := job.NewJob(payload, 0, time.Now(), 5, nil)
		if err := repo.CreateJob(ctx, j); err != nil {
			log.Fatalf("create job %d: %v", i, err)
		}
		fmt.Printf("created job %s\n", j.ID)
	}

	// 2. Simulate two schedulers racing to claim the same jobs concurrently
	claim := func(name string) {
		jobs, err := repo.ClaimPendingJobs(ctx, 2)
		if err != nil {
			log.Printf("[%s] claim error: %v", name, err)
			return
		}
		fmt.Printf("[%s] claimed %d jobs:\n", name, len(jobs))
		for _, j := range jobs {
			fmt.Printf("  - %s status=%s\n", j.ID, j.Status)
		}
	}

	done := make(chan struct{})
	go func() {
		claim("scheduler-A")
		done <- struct{}{}
	}()
	go func() {
		claim("scheduler-B")
		done <- struct{}{}
	}()
	<-done
	<-done

	fmt.Println("\nIf scheduler-A and scheduler-B claimed different (non-overlapping) job IDs, SKIP LOCKED is working correctly.")
}
