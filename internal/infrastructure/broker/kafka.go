package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

// PingKafkaBrokers checks that at least one broker accepts TCP (writer does not connect at construction).
func PingKafkaBrokers(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no kafka brokers configured")
	}
	var d net.Dialer
	var lastErr error
	for _, addr := range brokers {
		c, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			lastErr = err
			continue
		}
		_ = c.Close()
		return nil
	}
	if lastErr != nil {
		return fmt.Errorf("kafka tcp dial: %w", lastErr)
	}
	return fmt.Errorf("no kafka brokers")
}

// KafkaPublisher writes to a single topic.
type KafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
		WriteTimeout:           10 * time.Second,
		AllowAutoTopicCreation: true,
	}
	return &KafkaPublisher{writer: w}
}

func (p *KafkaPublisher) Publish(ctx context.Context, payload []byte) error {
	slogPublish("kafka", "kafka:topic:"+p.writer.Topic, payload)
	return p.writer.WriteMessages(ctx, kafka.Message{Value: payload})
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}

// KafkaSubscriber reads from a topic using a consumer group.
type KafkaSubscriber struct {
	reader *kafka.Reader
}

func NewKafkaSubscriber(brokers []string, topic, groupID string) *KafkaSubscriber {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})
	return &KafkaSubscriber{reader: r}
}

func (s *KafkaSubscriber) Consume(ctx context.Context, handler func(ctx context.Context, payload []byte) error) error {
	for {
		m, err := s.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		slog.Debug("kafka record consumed", "broker_id", fmt.Sprintf("kafka:%d:%d", m.Partition, m.Offset))
		if err := handler(ctx, m.Value); err != nil {
			return err
		}
		if err := s.reader.CommitMessages(ctx, m); err != nil {
			return fmt.Errorf("kafka commit: %w", err)
		}
	}
}

func (s *KafkaSubscriber) Close() error {
	return s.reader.Close()
}
