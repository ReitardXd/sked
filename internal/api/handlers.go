package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/reitard/sked/internal/job"
)

type Handlers struct {
	repo *job.Repository
}

func NewHandlers(repo *job.Repository) *Handlers {
	return &Handlers{repo: repo}
}

type createJobRequest struct {
	Payload        json.RawMessage `json:"payload"`
	Priority       int16           `json:"priority"`
	RunAt          *time.Time      `json:"run_at,omitempty"`
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
}

func (h *Handlers) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if len(req.Payload) == 0 {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}
	if req.MaxAttempts <= 0 {
		req.MaxAttempts = 5
	}
	runAt := time.Now()
	if req.RunAt != nil {
		runAt = *req.RunAt
	}

	j := job.NewJob(req.Payload, req.Priority, runAt, req.MaxAttempts, req.IdempotencyKey)
	if err := h.repo.CreateJob(r.Context(), j); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}
	writeJSON(w, http.StatusCreated, j)
}

func (h *Handlers) GetJob(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	j, err := h.repo.GetJob(r.Context(), id)
	if errors.Is(err, job.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to fetch job")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (h *Handlers) HealthCheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
