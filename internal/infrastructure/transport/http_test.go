package transport

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"stuurwiel/internal/application/publish"
)

type httpMockPublisher struct {
	last []byte
	err  error
}

func (m *httpMockPublisher) Publish(ctx context.Context, payload []byte) error {
	if m.err != nil {
		return m.err
	}
	m.last = append([]byte(nil), payload...)
	return nil
}

func (m *httpMockPublisher) Close() error { return nil }

func TestHTTPPublishIntegration(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	n, k, r := &httpMockPublisher{}, &httpMockPublisher{}, &httpMockPublisher{}
	svc := publish.NewService(publish.NewPublishRouter(n, k, r))

	mux := http.NewServeMux()
	RegisterHTTP(mux, log, svc)

	t.Run("healthz 200", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
	})
	t.Run("openapi.yaml 200", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "openapi:") {
			t.Fatalf("expected yaml body")
		}
	})
	t.Run("swagger 200", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/swagger", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "swagger-ui") {
			t.Fatalf("expected swagger ui html")
		}
	})
	t.Run("nats 204", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/publish/nats", strings.NewReader(`{"event_id":42,"text":"hi"}`))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(string(n.last), `"event_id":42`) {
			t.Fatalf("unexpected payload %q", n.last)
		}
	})
	t.Run("kafka 204", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/publish/kafka", strings.NewReader(`{"event_id":7,"text":"k"}`))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status %d", rec.Code)
		}
		if !strings.Contains(string(k.last), `"event_id":7`) {
			t.Fatalf("unexpected payload %q", k.last)
		}
	})
	t.Run("rabbitmq 204", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/publish/rabbitmq", strings.NewReader(`{"event_id":1,"text":"r"}`))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status %d", rec.Code)
		}
		if !strings.Contains(string(r.last), `"text":"r"`) {
			t.Fatalf("unexpected payload %q", r.last)
		}
	})
	t.Run("unknown broker 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/publish/unknown", strings.NewReader(`{"event_id":1,"text":"x"}`))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status %d", rec.Code)
		}
	})
	t.Run("invalid json 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/publish/nats", strings.NewReader(`not json`))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status %d", rec.Code)
		}
	})
	t.Run("publish error 502", func(t *testing.T) {
		bad := &httpMockPublisher{err: errors.New("broker down")}
		svc := publish.NewService(publish.NewPublishRouter(bad, &httpMockPublisher{}, &httpMockPublisher{}))
		m := http.NewServeMux()
		RegisterHTTP(m, log, svc)
		req := httptest.NewRequest(http.MethodPost, "/v1/publish/nats", strings.NewReader(`{"event_id":1,"text":"x"}`))
		rec := httptest.NewRecorder()
		m.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status %d", rec.Code)
		}
	})
}
