package reconnect

import (
	"errors"
	"fmt"
)

// ErrReconnectLimit is returned when RecordFailure exceeds the configured maximum.
var ErrReconnectLimit = errors.New("reconnect attempt limit exceeded")

// AttemptLimiter counts failed dial/session attempts; RecordFailure returns an error after max failures.
type AttemptLimiter struct {
	max int
	n   int
}

// NewAttemptLimiter creates a limiter allowing at most max recorded failures (each RecordFailure increments).
func NewAttemptLimiter(max int) *AttemptLimiter {
	return &AttemptLimiter{max: max}
}

// RecordFailure increments the failure count and returns an error when the limit is exceeded.
func (l *AttemptLimiter) RecordFailure() error {
	l.n++
	if l.n > l.max {
		return fmt.Errorf("%w (%d attempts)", ErrReconnectLimit, l.max)
	}
	return nil
}

// Failures returns how many failed dial/session attempts have been recorded (after the last RecordFailure).
func (l *AttemptLimiter) Failures() int {
	return l.n
}

// Max returns the configured maximum number of failures before giving up.
func (l *AttemptLimiter) Max() int {
	return l.max
}
