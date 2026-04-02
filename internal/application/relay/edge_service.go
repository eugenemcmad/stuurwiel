package relay

import (
	"context"
	"log/slog"
	"sync"

	"stuurwiel/internal/domain"
	"stuurwiel/internal/logging"
)

// EdgeRelay consumes from Source and publishes to Sink when ForwardPolicy allows.
type EdgeRelay struct {
	Log         *slog.Logger
	SourceLabel string
	SinkLabel   string
	Source      MessageSource
	Sink        MessageSink
	Policy      ForwardPolicy
	// Concurrent handleMessage goroutines; 0 means default (10).
	Workers int
}

// Run blocks until ctx is done or Consume returns an error.
func (e *EdgeRelay) Run(ctx context.Context) error {
	n := e.Workers
	if n <= 0 {
		n = 10
	}
	if n == 1 {
		return e.Source.Consume(ctx, e.handleMessage)
	}
	return e.runWithWorkerPool(ctx, n)
}

func (e *EdgeRelay) runWithWorkerPool(ctx context.Context, n int) error {
	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	sem := make(chan struct{}, n)
	var firstErr error
	var mu sync.Mutex

	wrap := func(_ context.Context, payload []byte) error {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() {
				<-sem
				wg.Done()
			}()
			if err := e.handleMessage(innerCtx, payload); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				mu.Unlock()
			}
		}()
		return nil
	}

	err := e.Source.Consume(innerCtx, wrap)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return err
}

func (e *EdgeRelay) handleMessage(ctx context.Context, payload []byte) error {
	msg, err := domain.Decode(payload)
	if err != nil {
		e.Log.Error("invalid domain message", "from", e.SourceLabel, "err", err)
		return err
	}

	if !e.Policy.ShouldForward() {
		e.Log.Info("message completed",
			"from", e.SourceLabel,
			"to", e.SinkLabel,
			logging.GroupMsg("msg", msg),
			"status", "completed",
			"action", "dropped_after_processing",
		)
		return nil
	}

	outMsg := msg
	outMsg.AppendHopLabel(e.SinkLabel)

	out, err := outMsg.Encode()
	if err != nil {
		return err
	}
	if err := e.Sink.Publish(ctx, out); err != nil {
		e.Log.Error("relay: sink publish failed", "from", e.SourceLabel, "to", e.SinkLabel, logging.GroupMsg("msg", outMsg), "err", err)
		return err
	}
	e.Log.Info("relay: domain Msg published to sink (next broker)",
		"op", "relay_sink_publish",
		"from", e.SourceLabel,
		"to", e.SinkLabel,
		logging.GroupMsg("msg", outMsg),
		"bytes", len(out),
	)
	return nil
}
