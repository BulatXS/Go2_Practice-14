package http

import "net/http"

func SessionAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sessionCookie, err := r.Cookie("session")
		if err != nil || sessionCookie.Value != "demo-session" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}
