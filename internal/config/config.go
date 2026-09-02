package config

import (
	"fmt"
	"os"
)

type Config struct {
	DatabaseURL string
	RedisAddr   string
	APIPort     string
}

func Load() (*Config, error) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/djs?sslmode=disable"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}
	if dbURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	return &Config{DatabaseURL: dbURL, RedisAddr: redisAddr, APIPort: port}, nil
}
