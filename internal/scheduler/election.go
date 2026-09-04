package scheduler

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	leaderKey  = "djs:scheduler:leader"
	leaseTTL   = 10 * time.Second
	renewEvery = 3 * time.Second
)

// LeaderElector implements a simple distributed lock over Redis using
// SET NX PX for acquisition and a Lua script for safe renewal/release
// (only the current holder can renew or release its own lease).
type LeaderElector struct {
	client   *redis.Client
	id       string // unique per scheduler instance, e.g. hostname+pid
	isLeader bool
}

func NewLeaderElector(client *redis.Client) *LeaderElector {
	return &LeaderElector{
		client: client,
		id:     uuid.NewString(),
	}
}

// Run blocks, continuously attempting to acquire leadership if not already
// leader, and renewing the lease while leader. Call IsLeader() from the
// scheduler's dispatch loop to gate claiming/dispatching.
func (e *LeaderElector) Run(ctx context.Context) {
	ticker := time.NewTicker(renewEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if e.isLeader {
				e.release(context.Background())
			}
			return
		case <-ticker.C:
			e.tick(ctx)
		}
	}
}

func (e *LeaderElector) tick(ctx context.Context) {
	if e.isLeader {
		renewed, err := e.renew(ctx)
		if err != nil {
			log.Printf("election: renew error: %v", err)
		}
		if !renewed {
			log.Printf("election: lost leadership (renew failed)")
			e.isLeader = false
		}
		return
	}

	acquired, err := e.acquire(ctx)
	if err != nil {
		log.Printf("election: acquire error: %v", err)
		return
	}
	if acquired {
		log.Printf("election: became leader (id=%s)", e.id)
		e.isLeader = true
	}
}

func (e *LeaderElector) IsLeader() bool {
	return e.isLeader
}

func (e *LeaderElector) acquire(ctx context.Context) (bool, error) {
	ok, err := e.client.SetNX(ctx, leaderKey, e.id, leaseTTL).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

// renewScript only extends the TTL if this instance still holds the lock —
// prevents a scheduler that was briefly network-partitioned from
// re-extending a lease that's since been claimed by someone else.
var renewScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	else
		return 0
	end
`)

func (e *LeaderElector) renew(ctx context.Context) (bool, error) {
	res, err := renewScript.Run(ctx, e.client, []string{leaderKey}, e.id, leaseTTL.Milliseconds()).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}
	return res == 1, nil
}

var releaseScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`)

func (e *LeaderElector) release(ctx context.Context) {
	if _, err := releaseScript.Run(ctx, e.client, []string{leaderKey}, e.id).Result(); err != nil {
		log.Printf("election: release error: %v", err)
	}
}
