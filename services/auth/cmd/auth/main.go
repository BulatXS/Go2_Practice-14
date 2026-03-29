package main

import (
	"net"
	"net/http"
	"os"

	authpb "singularity.com/pr14/proto/authpb"
	grpcapi "singularity.com/pr14/services/auth/internal/grpcapi"
	httpapi "singularity.com/pr14/services/auth/internal/http"
	sharedlogger "singularity.com/pr14/shared/logger"

	"google.golang.org/grpc"
)

func main() {
	log := sharedlogger.New("auth")
	serviceLog := sharedlogger.WithService(log, "auth")

	httpPort := os.Getenv("AUTH_PORT")
	if httpPort == "" {
		httpPort = "8081"
	}

	grpcPort := os.Getenv("AUTH_GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	httpAddr := ":" + httpPort
	grpcAddr := ":" + grpcPort

	go func() {
		handler := httpapi.NewRouter(log)

		serviceLog.WithField("component", "startup").
			WithField("transport", "http").
			WithField("addr", httpAddr).
			Info("service started")

		if err := http.ListenAndServe(httpAddr, handler); err != nil {
			serviceLog.WithField("component", "startup").
				WithField("transport", "http").
				WithField("error", err.Error()).
				Error("service failed")
			os.Exit(1)
		}
	}()

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		serviceLog.WithField("component", "startup").
			WithField("transport", "grpc").
			WithField("error", err.Error()).
			Error("listen failed")
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	authpb.RegisterAuthServiceServer(grpcServer, grpcapi.NewServer(log))

	serviceLog.WithField("component", "startup").
		WithField("transport", "grpc").
		WithField("addr", grpcAddr).
		Info("service started")

	if err := grpcServer.Serve(lis); err != nil {
		serviceLog.WithField("component", "startup").
			WithField("transport", "grpc").
			WithField("error", err.Error()).
			Error("service failed")
		os.Exit(1)
	}
}
