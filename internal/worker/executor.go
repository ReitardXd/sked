package worker

import (
	"encoding/json"
	"fmt"

	"github.com/reitard/sked/internal/job"
)

// Execute runs the job's actual work. For now this is a stub that just
// echoes the payload back as the result — replace with real dispatch logic
// (e.g. lookup job type, call the right handler) once you have real job
// types to run.
func Execute(j *job.Job) (json.RawMessage, error) {
	var payload map[string]any
	if err := json.Unmarshal(j.Payload, &payload); err != nil {
		return nil, fmt.Errorf("invalid payload: %w", err)
	}

	result, _ := json.Marshal(map[string]any{
		"echoed": payload,
		"note":   "stub executor — replace with real job dispatch",
	})
	return result, nil
}
