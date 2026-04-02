package broker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
)

// NATSPublisher publishes to a NATS subject.
type NATSPublisher struct {
	conn    *nats.Conn
	subject string
}

func NewNATSPublisher(url, subject string) (*NATSPublisher, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	return &NATSPublisher{conn: nc, subject: subject}, nil
}

func (p *NATSPublisher) Publish(ctx context.Context, payload []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	slogPublish("nats", "nats:"+p.subject, payload)
	return p.conn.Publish(p.subject, payload)
}

func (p *NATSPublisher) Close() error {
	p.conn.Close()
	return nil
}

// NATSSubscriber subscribes with a queue group for load-balanced consumers.
type NATSSubscriber struct {
	conn    *nats.Conn
	subject string
	queue   string
}

func NewNATSSubscriber(url, subject, queue string) (*NATSSubscriber, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	return &NATSSubscriber{conn: nc, subject: subject, queue: queue}, nil
}

func (s *NATSSubscriber) Consume(ctx context.Context, handler func(ctx context.Context, payload []byte) error) error {
	sub, err := s.conn.QueueSubscribeSync(s.subject, s.queue)
	if err != nil {
		return fmt.Errorf("nats queue subscribe: %w", err)
	}
	defer sub.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("nats next: %w", err)
		}
		slog.Debug("nats message received", "broker_id", fmt.Sprintf("nats:%s", msg.Subject))
		if err := handler(ctx, msg.Data); err != nil {
			return err
		}
	}
}

func (s *NATSSubscriber) Close() error {
	s.conn.Close()
	return nil
}
