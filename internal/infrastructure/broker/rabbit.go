package broker

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitPublisher declares a queue and publishes to it.
type RabbitPublisher struct {
	url   string
	queue string
	conn  *amqp.Connection
	ch    *amqp.Channel
}

func NewRabbitPublisher(url, queue string) (*RabbitPublisher, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("rabbit dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("rabbit channel: %w", err)
	}
	_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("rabbit declare queue: %w", err)
	}
	return &RabbitPublisher{url: url, queue: queue, conn: conn, ch: ch}, nil
}

func (p *RabbitPublisher) Publish(ctx context.Context, payload []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	slogPublish("rabbitmq", "rabbit:"+p.queue, payload)
	return p.ch.PublishWithContext(ctx, "", p.queue, false, false, amqp.Publishing{
		ContentType:  "application/octet-stream",
		DeliveryMode: amqp.Persistent,
		Body:         payload,
	})
}

func (p *RabbitPublisher) Close() error {
	if p.ch != nil {
		p.ch.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

// RabbitSubscriber consumes from a durable queue with manual ack.
type RabbitSubscriber struct {
	url   string
	queue string
}

func NewRabbitSubscriber(url, queue string) *RabbitSubscriber {
	return &RabbitSubscriber{url: url, queue: queue}
}

func (s *RabbitSubscriber) Consume(ctx context.Context, handler func(ctx context.Context, payload []byte) error) error {
	conn, err := amqp.Dial(s.url)
	if err != nil {
		return fmt.Errorf("rabbit dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("rabbit channel: %w", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(s.queue, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("rabbit declare queue: %w", err)
	}

	if err := ch.Qos(10, 0, false); err != nil {
		return fmt.Errorf("rabbit qos: %w", err)
	}

	tag := "stuurwiel-relay"
	if h, err := os.Hostname(); err == nil && h != "" {
		tag = fmt.Sprintf("stuurwiel-%s", h)
	}
	msgs, err := ch.Consume(s.queue, tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("rabbit consume: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("rabbit channel closed")
			}
			slog.Debug("rabbit delivery received", "broker_id", fmt.Sprintf("rabbit:%d", d.DeliveryTag))
			err := handler(ctx, d.Body)
			if err != nil {
				_ = d.Nack(false, true)
				return err
			}
			if err := d.Ack(false); err != nil {
				return fmt.Errorf("rabbit ack: %w", err)
			}
		}
	}
}

func (s *RabbitSubscriber) Close() error {
	return nil
}
