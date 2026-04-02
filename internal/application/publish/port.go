package publish

import "context"

// Publisher is the outbound port: push a raw payload to a broker-specific destination.
type Publisher interface {
	Publish(ctx context.Context, payload []byte) error
	Close() error
}
