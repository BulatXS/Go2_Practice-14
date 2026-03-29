package middleware

import (
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *loggingResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func AccessLog(log *logrus.Entry) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			rw := &loggingResponseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			next.ServeHTTP(rw, r)

			requestID := GetRequestID(r.Context())
			durationMs := time.Since(start).Milliseconds()

			log.WithFields(logrus.Fields{
				"request_id":  requestID,
				"method":      r.Method,
				"path":        r.URL.Path,
				"status":      rw.statusCode,
				"duration_ms": durationMs,
			}).Info("request completed")
		})
	}
}
