package http

import (
	"net/http"

	sharedlogger "singularity.com/pr14/shared/logger"
	"singularity.com/pr14/shared/middleware"

	"github.com/sirupsen/logrus"
)

func NewRouter(log *logrus.Logger) http.Handler {
	handler := NewHandler()
	serviceLog := sharedlogger.WithService(log, "auth")

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/login", handler.Login)
	mux.HandleFunc("/v1/auth/verify", handler.Verify)

	return middleware.RequestID(
		middleware.AccessLog(serviceLog)(mux),
	)
}
