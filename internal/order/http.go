package order

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/eztwokey/wb-orders/internal/platform/correlation"
	platformmetrics "github.com/eztwokey/wb-orders/internal/platform/metrics"
	"github.com/google/uuid"
)

type HTTPHandler struct {
	service *Service
	logger  *slog.Logger
}

func NewHTTPHandler(service *Service, logger *slog.Logger) *HTTPHandler {
	return &HTTPHandler{service: service, logger: logger}
}

func (h *HTTPHandler) Routes(operational http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/orders", h.create)
	mux.HandleFunc("GET /api/v1/orders/{id}", h.get)
	mux.Handle("/", operational)
	return requestIDMiddleware(h.logger, metricsMiddleware(mux))
}

type createRequest struct {
	CustomerID  string `json:"customer_id"`
	WarehouseID string `json:"warehouse_id"`
	Items       []Item `json:"items"`
}

func (h *HTTPHandler) create(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	var request createRequest
	if err := decoder.Decode(&request); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_request", "request body is not valid JSON")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, r, http.StatusBadRequest, "invalid_request", "request body must contain one JSON object")
		return
	}
	customerID, err := uuid.Parse(request.CustomerID)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_request", "customer_id must be a UUID")
		return
	}
	warehouseID, err := uuid.Parse(request.WarehouseID)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_request", "warehouse_id must be a UUID")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	created, err := h.service.Create(ctx, CreateCommand{CustomerID: customerID, WarehouseID: warehouseID, Items: request.Items})
	if errors.Is(err, ErrInvalidRequest) {
		writeError(w, r, http.StatusUnprocessableEntity, "validation_error", err.Error())
		return
	}
	if err != nil {
		h.logger.ErrorContext(r.Context(), "create order failed", "error", err, "request_id", RequestID(r.Context()))
		writeError(w, r, http.StatusInternalServerError, "internal_error", "could not create order")
		return
	}
	w.Header().Set("Location", "/api/v1/orders/"+created.ID.String())
	writeJSON(w, http.StatusCreated, created)
}

func (h *HTTPHandler) get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_request", "order id must be a UUID")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	found, err := h.service.Get(ctx, id)
	if errors.Is(err, ErrNotFound) {
		writeError(w, r, http.StatusNotFound, "not_found", "order was not found")
		return
	}
	if err != nil {
		h.logger.ErrorContext(r.Context(), "get order failed", "error", err, "request_id", RequestID(r.Context()), "order_id", id)
		writeError(w, r, http.StatusInternalServerError, "internal_error", "could not get order")
		return
	}
	writeJSON(w, http.StatusOK, found)
}

type errorResponse struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id,omitempty"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	response := errorResponse{}
	response.Error.Code = code
	response.Error.Message = message
	response.Error.RequestID = RequestID(r.Context())
	writeJSON(w, status, response)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func RequestID(ctx context.Context) string {
	return correlation.RequestID(ctx)
}

func requestIDMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if requestID == "" || len(requestID) > 128 {
			requestID = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", requestID)
		ctx := correlation.WithRequestID(r.Context(), requestID)
		started := time.Now()
		next.ServeHTTP(w, r.WithContext(ctx))
		logger.InfoContext(ctx, "http request", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "duration", time.Since(started))
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		wrapped := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(wrapped, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		status := wrapped.status
		if status == 0 {
			status = http.StatusOK
		}
		platformmetrics.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
		platformmetrics.HTTPRequestDuration.WithLabelValues(r.Method, route).Observe(time.Since(started).Seconds())
	})
}

func ListenAndServe(ctx context.Context, addr string, handler http.Handler, logger *slog.Logger, shutdownTimeout time.Duration) error {
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	logger.Info("http server started", "address", addr)

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
		logger.Info("http server shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		return nil
	}
}
