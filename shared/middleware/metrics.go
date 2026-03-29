package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"singularity.com/pr14/shared/metrics"
)

type metricsResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *metricsResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func normalizeRoute(path string) string {
	switch {
	case path == "/v1/tasks":
		return "/v1/tasks"
	case strings.HasPrefix(path, "/v1/tasks/"):
		return "/v1/tasks/:id"
	case path == "/metrics":
		return "/metrics"
	default:
		return path
	}
}

func Metrics(m *metrics.HTTPMetrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			route := normalizeRoute(r.URL.Path)

			m.InFlight.Inc()
			start := time.Now()

			rw := &metricsResponseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			defer func() {
				m.InFlight.Dec()

				duration := time.Since(start).Seconds()
				status := strconv.Itoa(rw.statusCode)

				m.RequestsTotal.WithLabelValues(r.Method, route, status).Inc()
				m.RequestDuration.WithLabelValues(r.Method, route).Observe(duration)
			}()

			next.ServeHTTP(rw, r)
		})
	}
}
