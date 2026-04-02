package domain

import (
	"encoding/json"
	"fmt"
)

// Msg is the JSON domain message; relay appends Text only when forwarding to the next hop.
type Msg struct {
	EventID int64  `json:"event_id"`
	Text    string `json:"text"`
}

// Validate checks basic invariants. Call after JSON decode or when constructing from API fields.
func (m Msg) Validate() error {
	if m.EventID < 0 {
		return fmt.Errorf("%w: event_id must be >= 0", ErrInvalidMsg)
	}
	return nil
}

// AppendHopLabel appends a hop to Text. If label parses as Broker, uses HopPathSuffix; otherwise " -> <label>".
func (m *Msg) AppendHopLabel(label string) {
	if b, ok := ParseBrokerName(label); ok {
		m.Text += HopPathSuffix(b)
	} else {
		m.Text += " -> " + label
	}
}

// Encode serializes Msg to JSON bytes for broker payloads.
func (m Msg) Encode() ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("encode domain msg: %w", err)
	}
	return b, nil
}

// Decode parses JSON bytes into Msg.
func Decode(data []byte) (Msg, error) {
	var m Msg
	if err := json.Unmarshal(data, &m); err != nil {
		return Msg{}, fmt.Errorf("%w: %w", ErrInvalidMsg, err)
	}
	return m, nil
}
