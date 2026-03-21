package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Config struct {
	Port                   string
	MonolithURL            string
	MoviesServiceURL       string
	EventsServiceURL       string
	GradualMigration       bool
	MoviesMigrationPercent int
}

type App struct {
	cfg               Config
	monolithProxy     *httputil.ReverseProxy
	moviesProxy       *httputil.ReverseProxy
	eventsProxy       *httputil.ReverseProxy
	moviesAccumulator uint64
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	app, err := newApp(cfg)
	if err != nil {
		log.Fatalf("app init error: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", app.handleHealth)
	mux.HandleFunc("/", app.handleProxy)

	addr := ":" + cfg.Port
	server := &http.Server{
		Addr:              addr,
		Handler:           requestLogger(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("proxy-service started on %s", addr)
	log.Printf(
		"config: monolith=%s movies=%s events=%s gradual_migration=%t movies_migration_percent=%d",
		cfg.MonolithURL,
		cfg.MoviesServiceURL,
		cfg.EventsServiceURL,
		cfg.GradualMigration,
		cfg.MoviesMigrationPercent,
	)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}

func loadConfig() (Config, error) {
	cfg := Config{
		Port:             getEnv("PORT", "8000"),
		MonolithURL:      strings.TrimRight(os.Getenv("MONOLITH_URL"), "/"),
		MoviesServiceURL: strings.TrimRight(os.Getenv("MOVIES_SERVICE_URL"), "/"),
		EventsServiceURL: strings.TrimRight(os.Getenv("EVENTS_SERVICE_URL"), "/"),
	}

	if cfg.MonolithURL == "" {
		return Config{}, errors.New("MONOLITH_URL is required")
	}
	if cfg.MoviesServiceURL == "" {
		return Config{}, errors.New("MOVIES_SERVICE_URL is required")
	}
	if cfg.EventsServiceURL == "" {
		return Config{}, errors.New("EVENTS_SERVICE_URL is required")
	}

	gradualRaw := strings.TrimSpace(strings.ToLower(getEnv("GRADUAL_MIGRATION", "false")))
	switch gradualRaw {
	case "true":
		cfg.GradualMigration = true
	case "false":
		cfg.GradualMigration = false
	default:
		return Config{}, fmt.Errorf("GRADUAL_MIGRATION must be true or false, got %q", gradualRaw)
	}

	percentRaw := getEnv("MOVIES_MIGRATION_PERCENT", "0")
	percent, err := strconv.Atoi(percentRaw)
	if err != nil {
		return Config{}, fmt.Errorf("MOVIES_MIGRATION_PERCENT must be an integer, got %q", percentRaw)
	}
	if percent < 0 || percent > 100 {
		return Config{}, fmt.Errorf("MOVIES_MIGRATION_PERCENT must be between 0 and 100, got %d", percent)
	}
	cfg.MoviesMigrationPercent = percent

	return cfg, nil
}

func newApp(cfg Config) (*App, error) {
	monolithProxy, err := newReverseProxy(cfg.MonolithURL, "monolith")
	if err != nil {
		return nil, fmt.Errorf("create monolith proxy: %w", err)
	}

	moviesProxy, err := newReverseProxy(cfg.MoviesServiceURL, "movies-service")
	if err != nil {
		return nil, fmt.Errorf("create movies proxy: %w", err)
	}

	eventsProxy, err := newReverseProxy(cfg.EventsServiceURL, "events-service")
	if err != nil {
		return nil, fmt.Errorf("create events proxy: %w", err)
	}

	return &App{
		cfg:           cfg,
		monolithProxy: monolithProxy,
		moviesProxy:   moviesProxy,
		eventsProxy:   eventsProxy,
	}, nil
}

func newReverseProxy(rawBaseURL string, targetName string) (*httputil.ReverseProxy, error) {
	targetURL, err := url.Parse(rawBaseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid url %q: %w", rawBaseURL, err)
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)

		req.Host = targetURL.Host
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("X-Forwarded-Proto", schemeOrHTTP(req))
		req.Header.Set("X-Proxy-Target", targetName)
	}

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("proxy error target=%s method=%s path=%s err=%v", targetName, r.Method, r.URL.Path, err)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}

	return proxy, nil
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Strangler Fig Proxy is healthy"))
}

func (a *App) handleProxy(w http.ResponseWriter, r *http.Request) {
	targetName, proxy := a.chooseProxy(r)

	log.Printf(
		"route method=%s path=%s target=%s gradual_migration=%t movies_migration_percent=%d",
		r.Method,
		r.URL.Path,
		targetName,
		a.cfg.GradualMigration,
		a.cfg.MoviesMigrationPercent,
	)

	proxy.ServeHTTP(w, r)
}

func (a *App) chooseProxy(r *http.Request) (string, *httputil.ReverseProxy) {
	path := r.URL.Path

	switch {
	case path == "/api/movies/health":
		return "movies-service", a.moviesProxy

	case strings.HasPrefix(path, "/api/events"):
		return "events-service", a.eventsProxy

	case path == "/api/movies":
		if !a.cfg.GradualMigration {
			return "monolith", a.monolithProxy
		}
		if a.cfg.MoviesMigrationPercent <= 0 {
			return "monolith", a.monolithProxy
		}

		if a.cfg.MoviesMigrationPercent >= 100 {
			return "movies-service", a.moviesProxy
		}

		if shouldGoToMovies(&a.moviesAccumulator, a.cfg.MoviesMigrationPercent) {
			return "movies-service", a.moviesProxy
		}

		return "monolith", a.monolithProxy

	default:
		return "monolith", a.monolithProxy
	}
}

func shouldGoToMovies(acc *uint64, percent int) bool {
	for {
		current := atomic.LoadUint64(acc)
		next := current + uint64(percent)

		if next >= 100 {
			if atomic.CompareAndSwapUint64(acc, current, next-100) {
				return true
			}
			continue
		}

		if atomic.CompareAndSwapUint64(acc, current, next) {
			return false
		}
	}
}

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		log.Printf("incoming method=%s path=%s query=%q remote=%s", r.Method, r.URL.Path, r.URL.RawQuery, r.RemoteAddr)

		next.ServeHTTP(w, r)

		log.Printf("completed method=%s path=%s duration=%s", r.Method, r.URL.Path, time.Since(start))
	})
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func schemeOrHTTP(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	return "http"
}
