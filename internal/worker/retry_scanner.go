package worker

import (
	"context"
	"time"

	"github.com/marcusferl/etchflow/internal/store"
	"go.uber.org/zap"
)

// RetryScanner checks for RETRYING runs whose backoff period has expired and resets them to PENDING.
type RetryScanner struct {
	store         *store.Store
	logger        *zap.Logger
	checkInterval time.Duration
	done          chan struct{}
}

// NewRetryScanner creates a new RetryScanner instance.
func NewRetryScanner(store *store.Store, logger *zap.Logger, interval time.Duration) *RetryScanner {
	return &RetryScanner{
		store:         store,
		logger:        logger.With(zap.String("component", "retry_scanner")),
		checkInterval: interval,
		done:          make(chan struct{}),
	}
}

// Start begins the scanner loop in a background goroutine.
func (s *RetryScanner) Start(ctx context.Context) {
	s.logger.Info("starting retry scanner", zap.Duration("interval", s.checkInterval))
	go s.loop(ctx)
}

// Stop gracefully stops the scanner.
func (s *RetryScanner) Stop() {
	s.logger.Info("stopping retry scanner")
	close(s.done)
}

func (s *RetryScanner) loop(ctx context.Context) {
	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()

	// Initial check on startup
	s.scan(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-ticker.C:
			s.scan(ctx)
		}
	}
}

func (s *RetryScanner) scan(ctx context.Context) {
	count, err := s.store.Runs.WakeRetryingRuns(ctx)
	if err != nil {
		s.logger.Error("failed to wake retrying runs", zap.Error(err))
		return
	}
	if count > 0 {
		s.logger.Info("woke retrying runs", zap.Int64("count", count))
	}
}
