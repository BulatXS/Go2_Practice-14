package middleware

import (
	"net/http"
	"os"
)

func InstanceID(next http.Handler) http.Handler {
	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID = "unknown"
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Instance-ID", instanceID)
		next.ServeHTTP(w, r)
	})
}
