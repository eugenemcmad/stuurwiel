package transport

import (
	"io"
	"log/slog"
	"net/http"
	"strings"

	"stuurwiel/internal/application/publish"
)

// RegisterHTTP mounts publish routes on mux: POST /v1/publish/{broker}, GET /healthz,
// GET /openapi.yaml, GET /swagger (Swagger UI).
func RegisterHTTP(mux *http.ServeMux, log *slog.Logger, svc *publish.Service) {
	registerOpenAPI(mux)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /v1/publish/{broker}", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		raw := strings.TrimSpace(req.PathValue("broker"))
		body, err := io.ReadAll(io.LimitReader(req.Body, publish.MaxPublishBodyBytes))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		err = svc.PublishJSON(req.Context(), raw, body)
		if err != nil {
			if msg, code, ok := publish.MapPublishError(err); ok {
				http.Error(w, msg, code)
				return
			}
			log.Error("publish failed", "broker", raw, "err", err)
			http.Error(w, "publish failed", http.StatusBadGateway)
			return
		}
		log.Info("published via http", "broker", raw, "bytes", len(body))
		w.WriteHeader(http.StatusNoContent)
	})
}
