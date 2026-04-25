//go:build integration

package broker

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestIntegrationNATSPublishConsume(t *testing.T) {
	natsURL := envOr("NATS_URL", "nats://127.0.0.1:4222")
	subject := "stuurwiel.integration.nats." + fmt.Sprint(time.Now().UnixNano())
	queue := "stuurwiel-integration-nats-" + fmt.Sprint(time.Now().UnixNano())
	payload := []byte(`{"event_id":101,"text":"integration nats"}`)

	sub, err := NewNATSSubscriber(natsURL, subject, queue)
	if err != nil {
		t.Skipf("nats unavailable: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	pub, err := NewNATSPublisher(natsURL, subject)
	if err != nil {
		t.Skipf("nats publisher unavailable: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	gotCh := make(chan []byte, 1)
	errCh := make(chan error, 1)
	go func() {
		err := sub.Consume(ctx, func(ctx context.Context, payload []byte) error {
			cp := append([]byte(nil), payload...)
			gotCh <- cp
			cancel()
			return nil
		})
		errCh <- err
	}()

	time.Sleep(150 * time.Millisecond)
	if err := pub.Publish(ctx, payload); err != nil {
		t.Fatalf("publish nats: %v", err)
	}

	select {
	case got := <-gotCh:
		if string(got) != string(payload) {
			t.Fatalf("payload mismatch: got=%q want=%q", got, payload)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting nats message: %v", ctx.Err())
	}
}

func TestIntegrationKafkaPublishConsume(t *testing.T) {
	brokers := splitCSV(envOr("KAFKA_BROKERS", "127.0.0.1:9092"))
	topic := "stuurwiel-integration-kafka-" + fmt.Sprint(time.Now().UnixNano())
	groupID := "stuurwiel-integration-group-" + fmt.Sprint(time.Now().UnixNano())
	payload := []byte(`{"event_id":202,"text":"integration kafka"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if err := PingKafkaBrokers(ctx, brokers); err != nil {
		t.Skipf("kafka unavailable: %v", err)
	}

	sub := NewKafkaSubscriber(brokers, topic, groupID)
	t.Cleanup(func() { _ = sub.Close() })
	pub := NewKafkaPublisher(brokers, topic)
	t.Cleanup(func() { _ = pub.Close() })

	gotCh := make(chan []byte, 1)
	errCh := make(chan error, 1)
	go func() {
		err := sub.Consume(ctx, func(ctx context.Context, payload []byte) error {
			cp := append([]byte(nil), payload...)
			gotCh <- cp
			cancel()
			return nil
		})
		errCh <- err
	}()

	time.Sleep(200 * time.Millisecond)
	if err := pub.Publish(ctx, payload); err != nil {
		t.Fatalf("publish kafka: %v", err)
	}

	select {
	case got := <-gotCh:
		if string(got) != string(payload) {
			t.Fatalf("payload mismatch: got=%q want=%q", got, payload)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting kafka message: %v", ctx.Err())
	}
}

func TestIntegrationRabbitPublishConsume(t *testing.T) {
	rabbitURL := envOr("RABBIT_URL", "amqp://guest:guest@127.0.0.1:5672/")
	queue := "stuurwiel.integration.rabbit." + fmt.Sprint(time.Now().UnixNano())
	payload := []byte(`{"event_id":303,"text":"integration rabbit"}`)

	pub, err := NewRabbitPublisher(rabbitURL, queue)
	if err != nil {
		t.Skipf("rabbit unavailable: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	sub := NewRabbitSubscriber(rabbitURL, queue)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gotCh := make(chan []byte, 1)
	errCh := make(chan error, 1)
	go func() {
		err := sub.Consume(ctx, func(ctx context.Context, payload []byte) error {
			cp := append([]byte(nil), payload...)
			gotCh <- cp
			cancel()
			return nil
		})
		errCh <- err
	}()

	time.Sleep(200 * time.Millisecond)
	if err := pub.Publish(ctx, payload); err != nil {
		t.Fatalf("publish rabbit: %v", err)
	}

	select {
	case got := <-gotCh:
		if string(got) != string(payload) {
			t.Fatalf("payload mismatch: got=%q want=%q", got, payload)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting rabbit message: %v", ctx.Err())
	}
}

func envOr(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
