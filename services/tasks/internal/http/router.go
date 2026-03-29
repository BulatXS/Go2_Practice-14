package http

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"time"

	"singularity.com/pr14/services/tasks/internal/cache"
	"singularity.com/pr14/services/tasks/internal/rabbit"
	"singularity.com/pr14/services/tasks/internal/repository"
	"singularity.com/pr14/services/tasks/internal/service"
	sharedlogger "singularity.com/pr14/shared/logger"
	"singularity.com/pr14/shared/metrics"
	"singularity.com/pr14/shared/middleware"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

func NewRouter(log *logrus.Logger) http.Handler {
	serviceLog := sharedlogger.WithService(log, "tasks")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		serviceLog.WithFields(logrus.Fields{
			"component": "startup",
			"error":     "DATABASE_URL is empty",
		}).Error("failed to start tasks service")
		os.Exit(1)
	}

	db, err := repository.OpenPostgres(dbURL)
	if err != nil {
		serviceLog.WithFields(logrus.Fields{
			"component": "database",
			"error":     err.Error(),
		}).Error("failed to connect to database")
		os.Exit(1)
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	ttlSeconds := getEnvInt("CACHE_TTL_SECONDS", 120)
	jitterSeconds := getEnvInt("CACHE_TTL_JITTER_SECONDS", 30)

	redisClient := cache.NewRedisClient(redisAddr)
	if redisClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := redisClient.Ping(ctx).Err(); err != nil {
			serviceLog.WithFields(logrus.Fields{
				"component": "redis",
				"error":     err.Error(),
				"addr":      redisAddr,
			}).Warn("redis unavailable on startup, continuing with database fallback")
		}
	}

	rabbitURL := os.Getenv("RABBIT_URL")
	queueName := os.Getenv("QUEUE_NAME")

	var producer *rabbit.Producer
	if rabbitURL != "" && queueName != "" {
		p, err := rabbit.NewProducer(rabbitURL, queueName)
		if err != nil {
			serviceLog.WithFields(logrus.Fields{
				"component": "rabbitmq",
				"error":     err.Error(),
			}).Warn("failed to connect to rabbitmq, continuing without producer")
		} else {
			producer = p
		}
	}

	repo := repository.NewPostgresTaskRepository(db)
	svc := service.NewTaskService(
		repo,
		producer,
		redisClient,
		time.Duration(ttlSeconds)*time.Second,
		time.Duration(jitterSeconds)*time.Second,
	)

	handler := NewHandler(svc, log)

	reg := prometheus.NewRegistry()
	httpMetrics := metrics.NewHTTPMetrics(reg)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tasks/search", handler.SearchTasks)
	mux.HandleFunc("/v1/tasks", handler.Tasks)
	mux.HandleFunc("/v1/tasks/", handler.TaskByID)
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	return middleware.RequestID(
		middleware.InstanceID(
			middleware.SecurityHeaders(
				middleware.Metrics(httpMetrics)(
					middleware.AccessLog(serviceLog)(
						middleware.CSRFMiddleware(mux),
					),
				),
			),
		),
	)
}

func getEnvInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}

	return value
}
