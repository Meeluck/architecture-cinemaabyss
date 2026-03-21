package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	TopicMovie   = "movie-events"
	TopicUser    = "user-events"
	TopicPayment = "payment-events"

	EventTypeMovie   = "movie"
	EventTypeUser    = "user"
	EventTypePayment = "payment"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

type HealthResponse struct {
	Status bool `json:"status"`
}

type Event struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Timestamp string         `json:"timestamp"`
	Payload   map[string]any `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description *string  `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int     `json:"user_id"`
	Username  *string `json:"username,omitempty"`
	Email     *string `json:"email,omitempty"`
	Action    string  `json:"action"`
	Timestamp string  `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int     `json:"payment_id"`
	UserID     int     `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Timestamp  string  `json:"timestamp"`
	MethodType *string `json:"method_type,omitempty"`
}

func ValidateMovieEvent(e MovieEvent) error {
	if e.MovieID <= 0 {
		return errors.New("movie_id is required and must be > 0")
	}
	if strings.TrimSpace(e.Title) == "" {
		return errors.New("title is required")
	}
	if strings.TrimSpace(e.Action) == "" {
		return errors.New("action is required")
	}
	return nil
}

func ValidateUserEvent(e UserEvent) error {
	if e.UserID <= 0 {
		return errors.New("user_id is required and must be > 0")
	}
	if strings.TrimSpace(e.Action) == "" {
		return errors.New("action is required")
	}
	if strings.TrimSpace(e.Timestamp) == "" {
		return errors.New("timestamp is required")
	}
	if _, err := time.Parse(time.RFC3339, e.Timestamp); err != nil {
		return errors.New("timestamp must be RFC3339 date-time")
	}
	return nil
}

func ValidatePaymentEvent(e PaymentEvent) error {
	if e.PaymentID <= 0 {
		return errors.New("payment_id is required and must be > 0")
	}
	if e.UserID <= 0 {
		return errors.New("user_id is required and must be > 0")
	}
	if e.Amount == 0 {
		return errors.New("amount is required and must not be 0")
	}
	if strings.TrimSpace(e.Status) == "" {
		return errors.New("status is required")
	}
	if strings.TrimSpace(e.Timestamp) == "" {
		return errors.New("timestamp is required")
	}
	if _, err := time.Parse(time.RFC3339, e.Timestamp); err != nil {
		return errors.New("timestamp must be RFC3339 date-time")
	}
	return nil
}

func BuildMovieDomainEvent(req MovieEvent) (Event, error) {
	payload, err := toPayloadMap(req)
	if err != nil {
		return Event{}, err
	}

	return Event{
		ID:        fmt.Sprintf("movie-%d-%s-%d", req.MovieID, sanitizeForID(req.Action), time.Now().UTC().UnixNano()),
		Type:      EventTypeMovie,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   payload,
	}, nil
}

func BuildUserDomainEvent(req UserEvent) (Event, error) {
	payload, err := toPayloadMap(req)
	if err != nil {
		return Event{}, err
	}

	return Event{
		ID:        fmt.Sprintf("user-%d-%s-%d", req.UserID, sanitizeForID(req.Action), time.Now().UTC().UnixNano()),
		Type:      EventTypeUser,
		Timestamp: req.Timestamp,
		Payload:   payload,
	}, nil
}

func BuildPaymentDomainEvent(req PaymentEvent) (Event, error) {
	payload, err := toPayloadMap(req)
	if err != nil {
		return Event{}, err
	}

	return Event{
		ID:        fmt.Sprintf("payment-%d-%s-%d", req.PaymentID, sanitizeForID(req.Status), time.Now().UTC().UnixNano()),
		Type:      EventTypePayment,
		Timestamp: req.Timestamp,
		Payload:   payload,
	}, nil
}

func toPayloadMap(v any) (map[string]any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}

	return payload, nil
}

func sanitizeForID(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.ReplaceAll(s, " ", "-")
	s = strings.ReplaceAll(s, "/", "-")
	s = strings.ReplaceAll(s, "_", "-")
	if s == "" {
		return "unknown"
	}
	return s
}
