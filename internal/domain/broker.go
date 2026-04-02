package domain

// Broker identifies a message broker in the ring and API routes.
type Broker string

const (
	NATS     Broker = "nats"
	Kafka    Broker = "kafka"
	RabbitMQ Broker = "rabbitmq"
)

// All returns brokers in ring order: NATS → Kafka → RabbitMQ → NATS …
func AllBrokers() []Broker {
	return []Broker{NATS, Kafka, RabbitMQ}
}

// NextBroker returns the broker that follows b in the ring.
func NextBroker(b Broker) Broker {
	switch b {
	case NATS:
		return Kafka
	case Kafka:
		return RabbitMQ
	case RabbitMQ:
		return NATS
	default:
		return NATS
	}
}

// ParseBrokerName maps API path strings to Broker (case-insensitive variants for known names).
func ParseBrokerName(s string) (Broker, bool) {
	switch s {
	case "nats", "NATS":
		return NATS, true
	case "kafka", "Kafka", "KAFKA":
		return Kafka, true
	case "rabbitmq", "rabbit", "RabbitMQ", "RABBITMQ":
		return RabbitMQ, true
	default:
		return "", false
	}
}

// HopPathSuffix is appended to Msg.Text when a worker forwards to the next broker.
func HopPathSuffix(target Broker) string {
	switch target {
	case NATS:
		return " -> NATS"
	case Kafka:
		return " -> Kafka"
	case RabbitMQ:
		return " -> RMQ"
	default:
		return " -> " + string(target)
	}
}
