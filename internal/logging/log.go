package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// Level parses LOG_LEVEL-style strings into slog levels.
func Level(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// New returns a slog logger with JSON output to stdout and the configured level.
func New(level string) *slog.Logger {
	return NewJSON(os.Stdout, level)
}

// Stderr returns a JSON slog logger to stderr (e.g. failures before config is loaded).
func Stderr(level string) *slog.Logger {
	return NewJSON(os.Stderr, level)
}

// NewJSON writes JSON logs to w with the given level.
func NewJSON(w io.Writer, level string) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: Level(level)})
	return slog.New(h)
}
