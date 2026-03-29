package main

import (
	"log"
	"net/http"
	"os"

	"singularity.com/pr14/services/graphql/graph"
	graphhttp "singularity.com/pr14/services/graphql/internal/http"
	"singularity.com/pr14/services/graphql/internal/repository"
	"singularity.com/pr14/services/graphql/internal/service"
	sharedlogger "singularity.com/pr14/shared/logger"
)

func main() {
	port := os.Getenv("GRAPHQL_PORT")
	if port == "" {
		port = "8090"
	}

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}

	logger := sharedlogger.New("graphql")

	db, err := repository.OpenPostgres(dsn)
	if err != nil {
		logger.Fatalf("failed to connect to postgres: %v", err)
	}

	repo := repository.New(db)
	svc := service.New(repo)

	router := graphhttp.NewRouter(&graph.Resolver{
		Service: svc,
	}, logger)

	logger.Infof("GraphQL server started on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, router))
}
