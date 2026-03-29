package grpcapi

import (
	"context"
	"strings"

	authpb "singularity.com/pr14/proto/authpb"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	DemoToken   = "demo-token"
	DemoSubject = "student"
)

type Server struct {
	authpb.UnimplementedAuthServiceServer
	log *logrus.Entry
}

func NewServer(log *logrus.Logger) *Server {
	return &Server{
		log: log.WithField("service", "auth"),
	}
}

func (s *Server) Verify(ctx context.Context, req *authpb.VerifyRequest) (*authpb.VerifyResponse, error) {
	var requestID string

	if md, ok := metadata.FromIncomingContext(ctx); ok {
		values := md.Get("x-request-id")
		if len(values) > 0 {
			requestID = values[0]
		}
	}

	token := strings.TrimSpace(req.GetToken())
	token = strings.TrimPrefix(token, "Bearer ")
	token = strings.TrimSpace(token)

	entry := s.log.WithFields(logrus.Fields{
		"component":  "grpc_verify",
		"request_id": requestID,
		"has_auth":   token != "",
	})

	entry.Info("verify called")

	if token == "" {
		entry.WithField("error", "missing token").Warn("unauthorized request")
		return nil, status.Error(codes.Unauthenticated, "missing token")
	}

	if token != DemoToken {
		entry.WithField("error", "invalid token").Warn("unauthorized request")
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}

	entry.WithField("subject", DemoSubject).Info("verify successful")

	return &authpb.VerifyResponse{
		Valid:   true,
		Subject: DemoSubject,
	}, nil
}
