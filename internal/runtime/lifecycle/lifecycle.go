package lifecycle

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

// RunContext runs fn until it finishes, ctx is cancelled, or a shutdown signal is received.
func RunContext(log *slog.Logger, fn func(context.Context) error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- fn(ctx) }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			log.Error("worker stopped", "err", err)
			os.Exit(1)
		}
	case <-sig:
		log.Info("shutdown signal")
		cancel()
		if err := <-errCh; err != nil && err != context.Canceled {
			log.Error("worker stopped", "err", err)
			os.Exit(1)
		}
	}
}
