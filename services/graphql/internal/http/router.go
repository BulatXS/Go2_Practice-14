package http

import (
	"net/http"

	"singularity.com/pr14/services/graphql/graph"
	sharedlogger "singularity.com/pr14/shared/logger"
	"singularity.com/pr14/shared/metrics"
	"singularity.com/pr14/shared/middleware"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

func NewRouter(resolver *graph.Resolver, log *logrus.Logger) http.Handler {
	serviceLog := sharedlogger.WithService(log, "graphql")

	srv := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{
		Resolvers: resolver,
	}))

	reg := prometheus.NewRegistry()
	httpMetrics := metrics.NewHTTPMetrics(reg)

	rootMux := http.NewServeMux()
	rootMux.Handle("/", playground.Handler("GraphQL Playground", "/graphql"))
	rootMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	queryHandler := middleware.RequestID(
		middleware.InstanceID(
			middleware.SecurityHeaders(
				middleware.Metrics(httpMetrics)(
					middleware.AccessLog(serviceLog)(
						SessionAuthMiddleware(
							middleware.CSRFMiddleware(srv),
						),
					),
				),
			),
		),
	)

	rootMux.Handle("/graphql", queryHandler)

	return rootMux
}
