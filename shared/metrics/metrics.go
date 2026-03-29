package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type HTTPMetrics struct {
	RequestsTotal   *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	InFlight        prometheus.Gauge
}

func NewHTTPMetrics(reg prometheus.Registerer) *HTTPMetrics {
	m := &HTTPMetrics{
		RequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_requests_total",
				Help: "Total number of HTTP requests.",
			},
			[]string{"method", "route", "status"},
		),
		RequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_request_duration_seconds",
				Help:    "HTTP request duration in seconds.",
				Buckets: []float64{0.01, 0.05, 0.1, 0.3, 1, 3},
			},
			[]string{"method", "route"},
		),
		InFlight: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "http_in_flight_requests",
				Help: "Current number of in-flight HTTP requests.",
			},
		),
	}

	reg.MustRegister(
		m.RequestsTotal,
		m.RequestDuration,
		m.InFlight,
	)

	return m
}
