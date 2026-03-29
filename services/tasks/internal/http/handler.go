package http

import (
	"encoding/json"
	"net/http"
	"strings"

	"singularity.com/pr14/services/tasks/internal/service"
	sharedlogger "singularity.com/pr14/shared/logger"
	"singularity.com/pr14/shared/middleware"

	"github.com/sirupsen/logrus"
)

type Handler struct {
	svc *service.TaskService
	log *logrus.Entry
}

func NewHandler(svc *service.TaskService, log *logrus.Logger) *Handler {
	return &Handler{
		svc: svc,
		log: sharedlogger.WithService(log, "tasks"),
	}
}

func (h *Handler) authorize(w http.ResponseWriter, r *http.Request) bool {
	sessionCookie, err := r.Cookie("session")
	if err != nil || sessionCookie.Value != "demo-session" {
		h.log.WithFields(logrus.Fields{
			"request_id": middleware.GetRequestID(r.Context()),
			"component":  "handler",
			"error":      "missing or invalid session cookie",
		}).Warn("unauthorized request")

		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}

	return true
}

func (h *Handler) Tasks(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r) {
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.GetAll(w, r)
	case http.MethodPost:
		h.CreateTask(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handler) TaskByID(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r) {
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.GetByID(w, r)
	case http.MethodPatch:
		h.UpdateTask(w, r)
	case http.MethodDelete:
		h.DeleteTask(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handler) SearchTasks(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r) {
		return
	}

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	title := r.URL.Query().Get("title")
	items, err := h.svc.SearchByTitleSafe(r.Context(), title)
	if err != nil {
		h.log.WithFields(logrus.Fields{
			"request_id": middleware.GetRequestID(r.Context()),
			"component":  "repository",
			"error":      err.Error(),
		}).Error("search failed")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, items)
}

func sanitizeDescription(input string) string {
	input = strings.ReplaceAll(input, "<", "")
	input = strings.ReplaceAll(input, ">", "")
	return input
}

func (h *Handler) CreateTask(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Title       string `json:"title"`
		Description string `json:"description"`
		DueDate     string `json:"due_date"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.log.WithFields(logrus.Fields{
			"request_id": middleware.GetRequestID(r.Context()),
			"component":  "handler",
			"error":      err.Error(),
		}).Warn("invalid request body")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(req.Title) == "" {
		h.log.WithFields(logrus.Fields{
			"request_id": middleware.GetRequestID(r.Context()),
			"component":  "handler",
			"error":      "title is required",
		}).Warn("validation failed")
		http.Error(w, "title is required", http.StatusBadRequest)
		return
	}

	req.Description = sanitizeDescription(req.Description)

	task, err := h.svc.Create(r.Context(), req.Title, req.Description, req.DueDate)
	if err != nil {
		h.log.WithFields(logrus.Fields{
			"request_id": middleware.GetRequestID(r.Context()),
			"component":  "repository",
			"error":      err.Error(),
		}).Error("create task failed")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusCreated, task)
}

func (h *Handler) GetAll(w http.ResponseWriter, r *http.Request) {
	items, err := h.svc.GetAll(r.Context())
	if err != nil {
		h.log.WithFields(logrus.Fields{
			"request_id": middleware.GetRequestID(r.Context()),
			"component":  "repository",
			"error":      err.Error(),
		}).Error("get all tasks failed")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) GetByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")

	task, ok, err := h.svc.GetByID(r.Context(), id)
	if err != nil {
		h.log.WithFields(logrus.Fields{
			"request_id": middleware.GetRequestID(r.Context()),
			"component":  "repository",
			"error":      err.Error(),
		}).Error("get task by id failed")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, task)
}

func (h *Handler) UpdateTask(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")

	var req struct {
		Title       *string `json:"title"`
		Description *string `json:"description"`
		DueDate     *string `json:"due_date"`
		Done        *bool   `json:"done"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.log.WithFields(logrus.Fields{
			"request_id": middleware.GetRequestID(r.Context()),
			"component":  "handler",
			"error":      err.Error(),
		}).Warn("invalid request body")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if req.Description != nil {
		sanitized := sanitizeDescription(*req.Description)
		req.Description = &sanitized
	}

	task, ok, err := h.svc.Update(r.Context(), id, req.Title, req.Description, req.DueDate, req.Done)
	if err != nil {
		h.log.WithFields(logrus.Fields{
			"request_id": middleware.GetRequestID(r.Context()),
			"component":  "repository",
			"error":      err.Error(),
		}).Error("update task failed")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, task)
}

func (h *Handler) DeleteTask(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")

	ok, err := h.svc.Delete(r.Context(), id)
	if err != nil {
		h.log.WithFields(logrus.Fields{
			"request_id": middleware.GetRequestID(r.Context()),
			"component":  "repository",
			"error":      err.Error(),
		}).Error("delete task failed")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
