package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/reitard/sked/internal/config"
	"github.com/reitard/sked/internal/job"
	"github.com/reitard/sked/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()

	repo := job.NewRepository(pool)
	w := worker.New(repo)

	log.Println("worker started, polling for jobs...")
	w.Run(ctx)
	log.Println("worker shutting down")
}
