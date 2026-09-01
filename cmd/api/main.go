package main

import (
	"context"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/reitard/sked/internal/api"
	"github.com/reitard/sked/internal/config"
	"github.com/reitard/sked/internal/job"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer pool.Close()

	repo := job.NewRepository(pool)
	handlers := api.NewHandlers(repo)
	router := api.NewRouter(handlers)

	log.Printf("api listening on :%s", cfg.APIPort)
	if err := http.ListenAndServe(":"+cfg.APIPort, router); err != nil {
		log.Fatalf("server: %v", err)
	}
}
