package publish

import (
	"context"
	"errors"
	"strings"
	"testing"

	"stuurwiel/internal/domain"
)

type stubPublisher struct {
	last []byte
	err  error
}

func (s *stubPublisher) Publish(ctx context.Context, payload []byte) error {
	if s.err != nil {
		return s.err
	}
	s.last = append([]byte(nil), payload...)
	return nil
}

func (s *stubPublisher) Close() error { return nil }

func TestPublishRouter_RoutesToBroker(t *testing.T) {
	ctx := context.Background()
	payload := []byte(`{"event_id":1,"text":"x"}`)

	t.Run("nats", func(t *testing.T) {
		n, k, r := &stubPublisher{}, &stubPublisher{}, &stubPublisher{}
		p := NewPublishRouter(n, k, r)
		if err := p.Publish(ctx, domain.NATS, payload); err != nil {
			t.Fatal(err)
		}
		if string(n.last) != string(payload) || len(k.last)+len(r.last) != 0 {
			t.Fatalf("wrong routing: nats=%q kafka=%q rabbit=%q", n.last, k.last, r.last)
		}
	})
	t.Run("kafka", func(t *testing.T) {
		n, k, r := &stubPublisher{}, &stubPublisher{}, &stubPublisher{}
		p := NewPublishRouter(n, k, r)
		if err := p.Publish(ctx, domain.Kafka, payload); err != nil {
			t.Fatal(err)
		}
		if string(k.last) != string(payload) || len(n.last)+len(r.last) != 0 {
			t.Fatalf("wrong routing")
		}
	})
	t.Run("rabbitmq", func(t *testing.T) {
		n, k, r := &stubPublisher{}, &stubPublisher{}, &stubPublisher{}
		p := NewPublishRouter(n, k, r)
		if err := p.Publish(ctx, domain.RabbitMQ, payload); err != nil {
			t.Fatal(err)
		}
		if string(r.last) != string(payload) || len(n.last)+len(k.last) != 0 {
			t.Fatalf("wrong routing")
		}
	})
}

func TestPublishRouter_UnknownBroker(t *testing.T) {
	p := NewPublishRouter(&stubPublisher{}, &stubPublisher{}, &stubPublisher{})
	err := p.Publish(context.Background(), domain.Broker("unknown"), []byte("{}"))
	if err == nil || !errors.Is(err, domain.ErrUnknownBroker) {
		t.Fatalf("expected ErrUnknownBroker, got %v", err)
	}
}

func TestPublishRouter_PublishError(t *testing.T) {
	want := errors.New("broker down")
	p := NewPublishRouter(&stubPublisher{err: want}, &stubPublisher{}, &stubPublisher{})
	err := p.Publish(context.Background(), domain.NATS, []byte("{}"))
	if !errors.Is(err, want) {
		t.Fatalf("got %v want %v", err, want)
	}
}

func TestService_PublishJSON(t *testing.T) {
	payload := []byte(`{"event_id":1,"text":"ok"}`)
	n := &stubPublisher{}
	svc := NewService(NewPublishRouter(n, &stubPublisher{}, &stubPublisher{}))
	if err := svc.PublishJSON(context.Background(), "nats", payload); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(n.last), `"text":"ok"`) {
		t.Fatalf("got %q", n.last)
	}
}

func TestService_MapPublishError(t *testing.T) {
	msg, code, ok := MapPublishError(domain.ErrUnknownBroker)
	if !ok || code != 400 || msg != "unknown broker" {
		t.Fatalf("got %q %d %v", msg, code, ok)
	}
}
