package domain

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestMsgRoundTrip(t *testing.T) {
	m := Msg{EventID: 42, Text: "hello"}
	b, err := m.Encode()
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	if raw["event_id"].(float64) != 42 {
		t.Fatalf("event_id: %v", raw["event_id"])
	}
	if raw["text"].(string) != "hello" {
		t.Fatalf("text: %v", raw["text"])
	}
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != m {
		t.Fatalf("decode: got %+v want %+v", got, m)
	}
}

func TestMsgValidate(t *testing.T) {
	if err := (Msg{EventID: 0, Text: "x"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (Msg{EventID: -1, Text: "x"}).Validate(); err == nil || !errors.Is(err, ErrInvalidMsg) {
		t.Fatalf("want ErrInvalidMsg, got %v", err)
	}
}

func TestDecodeInvalidJSON(t *testing.T) {
	_, err := Decode([]byte(`not json`))
	if err == nil || !errors.Is(err, ErrInvalidMsg) {
		t.Fatalf("got %v", err)
	}
}

func TestAppendHopLabel(t *testing.T) {
	m := Msg{EventID: 1, Text: "a"}
	m.AppendHopLabel("kafka")
	if m.Text != "a -> Kafka" {
		t.Fatalf("got %q", m.Text)
	}
	m2 := Msg{EventID: 1, Text: "b"}
	m2.AppendHopLabel("custom-sink")
	if m2.Text != "b -> custom-sink" {
		t.Fatalf("got %q", m2.Text)
	}
}
