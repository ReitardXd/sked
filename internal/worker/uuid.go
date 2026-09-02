package worker

import "github.com/google/uuid"

func uuidFromString(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}
