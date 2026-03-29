package http

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
)

type Handler struct{}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if req.Username != "student" || req.Password != "student" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	csrfToken, err := generateToken(32)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "session",
		Value:    "demo-session",
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   3600,
	})

	http.SetCookie(w, &http.Cookie{
		Name:     "csrf_token",
		Value:    csrfToken,
		Path:     "/",
		HttpOnly: false,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   3600,
	})

	writeJSON(w, http.StatusOK, map[string]string{
		"message": "login successful",
	})
}

func (h *Handler) Verify(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	if auth == "Bearer demo-token" {
		writeJSON(w, http.StatusOK, map[string]any{
			"valid":   true,
			"subject": "student",
		})
		return
	}

	http.Error(w, "unauthorized", http.StatusUnauthorized)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func generateToken(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
