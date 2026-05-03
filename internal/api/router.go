package api

import (
	"time"

	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/marcusferl/etchflow/internal/api/handler"
	"github.com/marcusferl/etchflow/internal/api/middleware"
	"go.uber.org/zap"
)

// NewRouter builds and returns the chi router with all routes and middleware.
func NewRouter(h *handler.Handlers, logger *zap.Logger) chi.Router {
	r := chi.NewRouter()

	// Global middleware — applied to every request
	r.Use(middleware.Recovery(logger))
	r.Use(middleware.RequestLogging(logger))
	r.Use(chiMiddleware.Timeout(30 * time.Second))

	// Routes
	r.Get("/health", h.Health)
	r.Get("/ready", h.Ready)

	// Run routes
	r.Post("/runs", h.CreateRun)
	r.Get("/runs/{id}", h.GetRun)
	r.Get("/runs/{id}/logs", h.GetLogs)
	r.Put("/runs/{id}/checkpoint", h.SaveCheckpoint)
	r.Get("/runs/{id}/checkpoints", h.GetCheckpoints)
	r.Get("/runs/{id}/state", h.GetState)
	r.Post("/runs/{id}/fail", h.FailRun)
	r.Post("/runs/{id}/complete", h.CompleteRun)
	r.Post("/runs/{id}/cancel", h.CancelRun)
	r.Put("/runs/{id}/heartbeat", h.UpdateHeartbeat)

	
	// Claim endpoint for the Pull-model Workers
	r.Post("/runs/claim", h.ClaimNextRun)

	return r
}
