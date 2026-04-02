package domain

import "errors"

var (
	// ErrUnknownBroker is returned when the broker name is not one of NATS, Kafka, RabbitMQ.
	ErrUnknownBroker = errors.New("unknown broker")
	// ErrInvalidMsg wraps decode/validation failures for Msg.
	ErrInvalidMsg = errors.New("invalid message")
)
