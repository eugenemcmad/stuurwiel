package transport

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	stuurwielv1 "stuurwiel/api/grpc/stuurwiel/v1"
	"stuurwiel/internal/application/publish"
	"stuurwiel/internal/domain"
)

// GRPCPublishServer implements stuurwielv1.MessageServiceServer.
type GRPCPublishServer struct {
	stuurwielv1.UnimplementedMessageServiceServer
	Log *slog.Logger
	Svc *publish.Service
}

// RegisterGRPC registers the publish service on the gRPC server.
func RegisterGRPC(s *grpc.Server, log *slog.Logger, svc *publish.Service) {
	stuurwielv1.RegisterMessageServiceServer(s, &GRPCPublishServer{Log: log, Svc: svc})
}

func (s *GRPCPublishServer) Publish(ctx context.Context, req *stuurwielv1.PublishRequest) (*stuurwielv1.PublishResponse, error) {
	b, n, err := s.Svc.PublishFromFields(ctx, req.GetBroker(), req.GetEventId(), req.GetText())
	if err != nil {
		if errors.Is(err, domain.ErrUnknownBroker) || errors.Is(err, domain.ErrInvalidMsg) {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		s.Log.Error("publish failed", "broker", req.GetBroker(), "err", err)
		return nil, status.Errorf(codes.Internal, "publish failed: %v", err)
	}
	return &stuurwielv1.PublishResponse{
		Broker:       string(b),
		BytesWritten: int32(n),
	}, nil
}
