package reconnect

import (
	"errors"
	"testing"
)

func TestAttemptLimiter_FailuresAndMax(t *testing.T) {
	l := NewAttemptLimiter(5)
	if l.Failures() != 0 || l.Max() != 5 {
		t.Fatalf("initial failures=%d max=%d", l.Failures(), l.Max())
	}
	_ = l.RecordFailure()
	if l.Failures() != 1 {
		t.Fatalf("failures=%d", l.Failures())
	}
}

func TestAttemptLimiter(t *testing.T) {
	l := NewAttemptLimiter(3)
	if err := l.RecordFailure(); err != nil {
		t.Fatal(err)
	}
	if err := l.RecordFailure(); err != nil {
		t.Fatal(err)
	}
	if err := l.RecordFailure(); err != nil {
		t.Fatal(err)
	}
	err := l.RecordFailure()
	if err == nil {
		t.Fatal("expected limit error")
	}
	if !errors.Is(err, ErrReconnectLimit) {
		t.Fatalf("got %v", err)
	}
}
