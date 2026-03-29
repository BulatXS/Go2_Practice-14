package main

import (
	"net/http"
	"os"

	httpapi "singularity.com/pr14/services/tasks/internal/http"
	sharedlogger "singularity.com/pr14/shared/logger"
)

func main() {
	log := sharedlogger.New("tasks")
	serviceLog := sharedlogger.WithService(log, "tasks")

	port := os.Getenv("TASKS_PORT")
	if port == "" {
		port = "8082"
	}

	addr := ":" + port

	serviceLog.WithField("component", "startup").
		WithField("addr", addr).
		Info("service started")

	if err := http.ListenAndServe(addr, httpapi.NewRouter(log)); err != nil {
		serviceLog.WithField("component", "startup").
			WithField("error", err.Error()).
			Error("service failed")
		os.Exit(1)
	}
}
