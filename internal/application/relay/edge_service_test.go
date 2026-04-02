package relay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"testing"

	"stuurwiel/internal/domain"
)

type mockSink struct {
	calls int
	err   error
	last  []byte
}

func (m *mockSink) Publish(ctx context.Context, payload []byte) error {
	m.last = append([]byte(nil), payload...)
	if m.err != nil {
		return m.err
	}
	m.calls++
	return nil
}

func (m *mockSink) Close() error { return nil }

type oneRoundSource struct {
	payload []byte
	after   chan struct{}
}

func (s *oneRoundSource) Consume(ctx context.Context, handler func(ctx context.Context, payload []byte) error) error {
	if err := handler(ctx, s.payload); err != nil {
		return err
	}
	if s.after != nil {
		close(s.after)
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *oneRoundSource) Close() error { return nil }

func TestEdgeRelayForwardsWhenPolicyAllows(t *testing.T) {
	sink := &mockSink{}
	after := make(chan struct{})
	src := &oneRoundSource{payload: []byte(`{"event_id":1,"text":"hello"}`), after: after}
	relay := &EdgeRelay{
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		SourceLabel: "nats",
		SinkLabel:   "kafka",
		Source:      src,
		Sink:        sink,
		Workers:     1,
		Policy:      &StochasticForwardPolicy{P: 0.8, RNG: rand.New(rand.NewSource(42))},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = relay.Run(ctx) }()
	<-after
	cancel()
	if sink.calls != 1 {
		t.Fatalf("expected 1 publish, got %d", sink.calls)
	}
	got, err := domain.Decode(sink.last)
	if err != nil || got.Text != "hello -> Kafka" {
		t.Fatalf("expected hop text, got %+v err %v", got, err)
	}
}

func TestEdgeRelayCompletesWhenPolicyDenies(t *testing.T) {
	sink := &mockSink{}
	after := make(chan struct{})
	src := &oneRoundSource{payload: []byte(`{"event_id":1,"text":"hello"}`), after: after}
	relay := &EdgeRelay{
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		SourceLabel: "nats",
		SinkLabel:   "kafka",
		Source:      src,
		Sink:        sink,
		Workers:     1,
		Policy:      &StochasticForwardPolicy{P: 0.1, RNG: rand.New(rand.NewSource(42))},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = relay.Run(ctx) }()
	<-after
	cancel()
	if sink.calls != 0 {
		t.Fatalf("expected 0 publishes, got %d", sink.calls)
	}
}

func TestEdgeRelay_InvalidPayloadStopsWithError(t *testing.T) {
	sink := &mockSink{}
	src := &oneRoundSource{payload: []byte(`not-json`), after: nil}
	relay := &EdgeRelay{
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		SourceLabel: "nats",
		SinkLabel:   "kafka",
		Source:      src,
		Sink:        sink,
		Workers:     1,
		Policy:      &StochasticForwardPolicy{P: 1, RNG: rand.New(rand.NewSource(1))},
	}
	err := relay.Run(context.Background())
	if err == nil {
		t.Fatal("expected error from invalid json")
	}
	if sink.calls != 0 {
		t.Fatalf("expected no publishes, got %d", sink.calls)
	}
}

func TestEdgeRelay_SinkErrorPropagates(t *testing.T) {
	sink := &mockSink{err: errors.New("kafka down")}
	src := &oneRoundSource{payload: []byte(`{"event_id":1,"text":"hello"}`), after: nil}
	relay := &EdgeRelay{
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		SourceLabel: "nats",
		SinkLabel:   "kafka",
		Source:      src,
		Sink:        sink,
		Workers:     1,
		Policy:      &StochasticForwardPolicy{P: 1, RNG: rand.New(rand.NewSource(1))},
	}
	err := relay.Run(context.Background())
	if !errors.Is(err, sink.err) {
		t.Fatalf("expected sink error, got %v", err)
	}
	got, decErr := domain.Decode(sink.last)
	if decErr != nil || got.Text != "hello -> Kafka" {
		t.Fatalf("expected hop appended before failure, got %+v err %v", got, decErr)
	}
}

func TestEdgeRelay_AppendsRMQSuffix(t *testing.T) {
	sink := &mockSink{}
	after := make(chan struct{})
	src := &oneRoundSource{payload: []byte(`{"event_id":1,"text":"start"}`), after: after}
	relay := &EdgeRelay{
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		SourceLabel: "kafka",
		SinkLabel:   "rabbitmq",
		Source:      src,
		Sink:        sink,
		Workers:     1,
		Policy:      &StochasticForwardPolicy{P: 1, RNG: rand.New(rand.NewSource(1))},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = relay.Run(ctx) }()
	<-after
	cancel()
	got, err := domain.Decode(sink.last)
	if err != nil || got.Text != "start -> RMQ" {
		t.Fatalf("got %+v err %v", got, err)
	}
}
