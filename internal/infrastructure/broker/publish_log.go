package broker

import (
	"log/slog"

	"stuurwiel/internal/domain"
	"stuurwiel/internal/logging"
)

// slogPublish logs decoded Msg on every outbound publish (API and workers).
func slogPublish(publisher string, brokerDest string, payload []byte) {
	if msg, err := domain.Decode(payload); err == nil {
		slog.Info(publisher+" publish", "broker_id", brokerDest, logging.GroupMsg("msg", msg), "bytes", len(payload))
	} else {
		slog.Warn(publisher+" publish", "broker_id", brokerDest, "bytes", len(payload), "decode_err", err.Error())
	}
}
