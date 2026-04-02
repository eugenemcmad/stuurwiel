package relay

import (
	"context"

	"stuurwiel/internal/application/publish"
)

// MessageSource is the inbound port (implemented by NATS/Kafka/Rabbit subscribers in infrastructure).
type MessageSource interface {
	Consume(ctx context.Context, handler func(ctx context.Context, payload []byte) error) error
	Close() error
}

// MessageSink is the outbound port (alias of publish.Publisher for relay naming).
type MessageSink = publish.Publisher

// ForwardPolicy abstracts forward vs drop without tying the use case to math/rand.
type ForwardPolicy interface {
	// ShouldForward returns true if the message should be published to the next broker.
	ShouldForward() bool
}
