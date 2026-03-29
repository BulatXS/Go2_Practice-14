package middleware

import (
	"net/http"
	"strings"
)

func CSRFMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost, http.MethodPatch, http.MethodDelete:
			if strings.HasPrefix(strings.TrimSpace(r.Header.Get("Authorization")), "Bearer ") {
				next.ServeHTTP(w, r)
				return
			}

			csrfCookie, err := r.Cookie("csrf_token")
			if err != nil || csrfCookie.Value == "" {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}

			headerToken := r.Header.Get("X-CSRF-Token")
			if headerToken == "" || headerToken != csrfCookie.Value {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}
