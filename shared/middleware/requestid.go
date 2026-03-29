package middleware

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

type contextKey string

const RequestIDKey contextKey = "request_id"
const HeaderRequestID = "X-Request-ID"

func GetRequestID(ctx context.Context) string {
	if v, ok := ctx.Value(RequestIDKey).(string); ok {
		return v
	}
	return ""
}

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get(HeaderRequestID)
		if requestID == "" {
			requestID = uuid.NewString()
		}

		w.Header().Set(HeaderRequestID, requestID)

		ctx := context.WithValue(r.Context(), RequestIDKey, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
