package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
)

type HTTPHandler struct {
	kafka *KafkaClient
}

func NewHTTPHandler(kafka *KafkaClient) *HTTPHandler {
	return &HTTPHandler{
		kafka: kafka,
	}
}

func (h *HTTPHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/events/health", h.handleHealth)
	mux.HandleFunc("/api/events/movie", h.handleCreateMovieEvent)
	mux.HandleFunc("/api/events/user", h.handleCreateUserEvent)
	mux.HandleFunc("/api/events/payment", h.handleCreatePaymentEvent)
}

func (h *HTTPHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "Method Not Allowed"})
		return
	}

	writeJSON(w, http.StatusOK, HealthResponse{Status: true})
}

func (h *HTTPHandler) handleCreateMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "Method Not Allowed"})
		return
	}

	var req MovieEvent
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	if err := ValidateMovieEvent(req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	event, err := BuildMovieDomainEvent(req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "failed to build event payload"})
		return
	}

	resp, err := h.kafka.Publish(TopicMovie, strconv.Itoa(req.MovieID), event)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "failed to publish event"})
		return
	}

	writeJSON(w, http.StatusCreated, resp)
}

func (h *HTTPHandler) handleCreateUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "Method Not Allowed"})
		return
	}

	var req UserEvent
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	if err := ValidateUserEvent(req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	event, err := BuildUserDomainEvent(req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "failed to build event payload"})
		return
	}

	resp, err := h.kafka.Publish(TopicUser, strconv.Itoa(req.UserID), event)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "failed to publish event"})
		return
	}

	writeJSON(w, http.StatusCreated, resp)
}

func (h *HTTPHandler) handleCreatePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "Method Not Allowed"})
		return
	}

	var req PaymentEvent
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	if err := ValidatePaymentEvent(req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	event, err := BuildPaymentDomainEvent(req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "failed to build event payload"})
		return
	}

	resp, err := h.kafka.Publish(TopicPayment, strconv.Itoa(req.PaymentID), event)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "failed to publish event"})
		return
	}

	writeJSON(w, http.StatusCreated, resp)
}

func decodeJSON(r *http.Request, dst any) error {
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}

	if decoder.More() {
		return errors.New("request body must contain a single JSON object")
	}

	return nil
}

func writeJSON(w http.ResponseWriter, statusCode int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("response encode error: %v", err)
	}
}

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Printf("incoming method=%s path=%s query=%q remote=%s", r.Method, r.URL.Path, r.URL.RawQuery, r.RemoteAddr)
		next.ServeHTTP(w, r)
		log.Printf("completed method=%s path=%s duration=%s", r.Method, r.URL.Path, time.Since(start))
	})
}
