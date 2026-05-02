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
	r.Post("/runs", h.CreateRun)
	r.Put("/runs/{id}/checkpoint", h.SaveCheckpoint)
	r.Get("/runs/{id}/state", h.GetState)

	return r
}
