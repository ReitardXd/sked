package queue

import "context"

// Queue is the abstraction between the scheduler (producer) and workers
// (consumers). Redis Streams and RabbitMQ both satisfy this without leaking
// broker-specific details into scheduler/worker code.
type Queue interface {
	// Publish enqueues a job ID for a worker to pick up.
	Publish(ctx context.Context, jobID string) error

	// Consume blocks and returns job IDs as they become available. The
	// returned ack func must be called once the job has been fully
	// processed (success or terminal failure) so the broker can mark the
	// message delivered.
	Consume(ctx context.Context, consumerName string) (<-chan Message, error)
}

type Message struct {
	JobID string
	Ack   func(ctx context.Context) error
}
