package logging

import (
	"log/slog"

	"stuurwiel/internal/domain"
)

// GroupMsg returns a structured slog attribute for domain.Msg (keeps domain free of slog).
func GroupMsg(key string, m domain.Msg) slog.Attr {
	return slog.Group(key,
		slog.Int64("event_id", m.EventID),
		slog.String("text", m.Text),
	)
}
