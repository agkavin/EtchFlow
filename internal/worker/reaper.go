package worker

import (
	"context"
	"time"

	"github.com/marcusferl/etchflow/internal/store"
	"go.uber.org/zap"
)

// Reaper scans for RUNNING runs that have missed their heartbeats and resets them to PENDING.
type Reaper struct {
	store          *store.Store
	logger         *zap.Logger
	checkInterval  time.Duration
	staleThreshold time.Duration
	done           chan struct{}
}

// NewReaper creates a new Reaper instance.
func NewReaper(store *store.Store, logger *zap.Logger, interval, threshold time.Duration) *Reaper {
	return &Reaper{
		store:          store,
		logger:         logger.With(zap.String("component", "reaper")),
		checkInterval:  interval,
		staleThreshold: threshold,
		done:           make(chan struct{}),
	}
}

// Start begins the reaper loop in a background goroutine.
func (r *Reaper) Start(ctx context.Context) {
	r.logger.Info("starting reaper", zap.Duration("interval", r.checkInterval), zap.Duration("threshold", r.staleThreshold))
	go r.loop(ctx)
}

// Stop gracefully stops the reaper.
func (r *Reaper) Stop() {
	r.logger.Info("stopping reaper")
	close(r.done)
}

func (r *Reaper) loop(ctx context.Context) {
	ticker := time.NewTicker(r.checkInterval)
	defer ticker.Stop()

	// Initial check on startup
	r.reap(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		case <-ticker.C:
			r.reap(ctx)
		}
	}
}

func (r *Reaper) reap(ctx context.Context) {
	count, err := r.store.Runs.ReapStaleRuns(ctx, r.staleThreshold)
	if err != nil {
		r.logger.Error("failed to reap stale runs", zap.Error(err))
		return
	}
	if count > 0 {
		r.logger.Info("reaped stale runs", zap.Int64("count", count))
	}
}
