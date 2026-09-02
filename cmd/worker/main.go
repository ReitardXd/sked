package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/reitard/sked/internal/config"
	"github.com/reitard/sked/internal/job"
	"github.com/reitard/sked/internal/queue"
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

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer redisClient.Close()

	repo := job.NewRepository(pool)
	q := queue.NewRedisQueue(redisClient)

	hostname, _ := os.Hostname()
	w := worker.New(repo, q, "worker-"+hostname)

	log.Println("worker started, consuming from queue...")
	w.Run(ctx)
	log.Println("worker shutting down")
}
