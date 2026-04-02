package transport

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	stuurwielv1 "stuurwiel/api/grpc/stuurwiel/v1"
	"stuurwiel/internal/application/publish"
	"stuurwiel/internal/domain"
)

type grpcMockPublisher struct {
	last []byte
	err  error
}

func (m *grpcMockPublisher) Publish(ctx context.Context, payload []byte) error {
	if m.err != nil {
		return m.err
	}
	m.last = append([]byte(nil), payload...)
	return nil
}

func (m *grpcMockPublisher) Close() error { return nil }

func TestGRPCPublishIntegration_RoutesPerBroker(t *testing.T) {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	cases := []struct {
		name     string
		broker   string
		wantNATS bool
		wantK    bool
		wantR    bool
	}{
		{"nats", "nats", true, false, false},
		{"kafka", "kafka", false, true, false},
		{"rabbitmq", "rabbitmq", false, false, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n, k, r := &grpcMockPublisher{}, &grpcMockPublisher{}, &grpcMockPublisher{}
			svc := publish.NewService(publish.NewPublishRouter(n, k, r))

			lis := bufconn.Listen(1024 * 1024)
			t.Cleanup(func() { _ = lis.Close() })
			srv := grpc.NewServer()
			RegisterGRPC(srv, log, svc)

			go func() {
				if err := srv.Serve(lis); err != nil {
					t.Logf("grpc serve: %v", err)
				}
			}()
			t.Cleanup(func() { srv.Stop() })

			dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
			conn, err := grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(dialer),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })

			cli := stuurwielv1.NewMessageServiceClient(conn)
			_, err = cli.Publish(ctx, &stuurwielv1.PublishRequest{
				Broker:  tc.broker,
				EventId: 99,
				Text:    "ping",
			})
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantNATS && len(n.last) == 0 || tc.wantK && len(k.last) == 0 || tc.wantR && len(r.last) == 0 {
				t.Fatalf("routing: n=%d k=%d r=%d", len(n.last), len(k.last), len(r.last))
			}
			if !tc.wantNATS && len(n.last) != 0 || !tc.wantK && len(k.last) != 0 || !tc.wantR && len(r.last) != 0 {
				t.Fatalf("unexpected side writes n=%d k=%d r=%d", len(n.last), len(k.last), len(r.last))
			}
			var payload []byte
			switch {
			case tc.wantNATS:
				payload = n.last
			case tc.wantK:
				payload = k.last
			default:
				payload = r.last
			}
			got, err := domain.Decode(payload)
			if err != nil || got.EventID != 99 || got.Text != "ping" {
				t.Fatalf("payload %+v err %v", got, err)
			}
		})
	}
}

func TestGRPCPublish_InvalidBroker(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mp := &grpcMockPublisher{}
	svc := publish.NewService(publish.NewPublishRouter(mp, mp, mp))

	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = lis.Close() })
	srv := grpc.NewServer()
	RegisterGRPC(srv, log, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop() })

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cli := stuurwielv1.NewMessageServiceClient(conn)
	_, err = cli.Publish(context.Background(), &stuurwielv1.PublishRequest{
		Broker: "unknown",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("got %v", err)
	}
}

func TestGRPCPublish_PublisherError(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	bad := &grpcMockPublisher{err: errors.New("broker down")}
	svc := publish.NewService(publish.NewPublishRouter(bad, &grpcMockPublisher{}, &grpcMockPublisher{}))

	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = lis.Close() })
	srv := grpc.NewServer()
	RegisterGRPC(srv, log, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop() })

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cli := stuurwielv1.NewMessageServiceClient(conn)
	_, err = cli.Publish(context.Background(), &stuurwielv1.PublishRequest{
		Broker:  "nats",
		EventId: 1,
		Text:    "x",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal {
		t.Fatalf("got %v", err)
	}
}
