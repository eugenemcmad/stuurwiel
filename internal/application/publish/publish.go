package publish

import (
	"context"
	"fmt"

	"stuurwiel/internal/domain"
)

// PublishRouter dispatches to a broker by domain.Broker.
type PublishRouter struct {
	publishers map[string]Publisher
}

// NewPublishRouter wires publishers for the three ring brokers.
func NewPublishRouter(nats, kafka, rabbit Publisher) *PublishRouter {
	return &PublishRouter{
		publishers: map[string]Publisher{
			string(domain.NATS):     nats,
			string(domain.Kafka):    kafka,
			string(domain.RabbitMQ): rabbit,
		},
	}
}

// Publish sends payload to the named broker.
func (p *PublishRouter) Publish(ctx context.Context, broker domain.Broker, payload []byte) error {
	pub, ok := p.publishers[string(broker)]
	if !ok {
		return fmt.Errorf("%w: %q", domain.ErrUnknownBroker, broker)
	}
	return pub.Publish(ctx, payload)
}
