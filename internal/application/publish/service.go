package publish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"stuurwiel/internal/domain"
)

// MaxPublishBodyBytes is the maximum JSON body size for HTTP publish.
const MaxPublishBodyBytes = 4 << 20

// Service is the publish use case: validate and route to the correct broker publisher.
type Service struct {
	Router *PublishRouter
}

// NewService wires the publish use case.
func NewService(r *PublishRouter) *Service {
	return &Service{Router: r}
}

// PublishJSON decodes a domain message from JSON and publishes to the named broker.
func (s *Service) PublishJSON(ctx context.Context, brokerPathSegment string, body []byte) error {
	b, ok := domain.ParseBrokerName(strings.TrimSpace(brokerPathSegment))
	if !ok {
		return fmt.Errorf("%w: %q", domain.ErrUnknownBroker, brokerPathSegment)
	}
	var m domain.Msg
	if err := json.Unmarshal(body, &m); err != nil {
		return fmt.Errorf("%w: %w", domain.ErrInvalidMsg, err)
	}
	if err := m.Validate(); err != nil {
		return err
	}
	payload, err := m.Encode()
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	return s.Router.Publish(ctx, b, payload)
}

// PublishFromFields builds Msg from API fields and publishes (gRPC and similar adapters).
// On success returns the resolved broker and encoded payload size in bytes.
func (s *Service) PublishFromFields(ctx context.Context, broker string, eventID int64, text string) (domain.Broker, int, error) {
	b, ok := domain.ParseBrokerName(strings.TrimSpace(broker))
	if !ok {
		return "", 0, fmt.Errorf("%w: %q", domain.ErrUnknownBroker, broker)
	}
	m := domain.Msg{EventID: eventID, Text: text}
	if err := m.Validate(); err != nil {
		return "", 0, err
	}
	payload, err := m.Encode()
	if err != nil {
		return "", 0, fmt.Errorf("encode: %w", err)
	}
	if err := s.Router.Publish(ctx, b, payload); err != nil {
		return "", 0, err
	}
	return b, len(payload), nil
}

// MapPublishError classifies errors for HTTP status mapping (returns false if not recognized).
func MapPublishError(err error) (msg string, status int, ok bool) {
	switch {
	case err == nil:
		return "", 0, false
	case errors.Is(err, domain.ErrUnknownBroker):
		return "unknown broker", 400, true
	case errors.Is(err, domain.ErrInvalidMsg):
		return "invalid message", 400, true
	default:
		return "", 0, false
	}
}
