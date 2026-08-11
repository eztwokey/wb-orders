package order

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCreateOrderHTTP(t *testing.T) {
	t.Parallel()
	store := &recordingStore{}
	handler := NewHTTPHandler(NewService(store), slog.New(slog.NewTextHandler(io.Discard, nil))).Routes(http.NotFoundHandler())
	body := `{"customer_id":"3f2b598f-bcaf-41f0-a4f4-7e80dc93d49a","warehouse_id":"1e734f9d-1855-4b60-a428-1645946878e1","items":[{"sku":"book","quantity":2,"unit_price_minor":500}]}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/orders", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Request-ID", "http-test-request")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", response.Code, response.Body.String())
	}
	if location := response.Header().Get("Location"); !strings.Contains(location, store.order.ID.String()) {
		t.Fatalf("Location = %q, want order id", location)
	}
	if store.event.RequestID != "http-test-request" {
		t.Fatalf("event request_id = %q, want propagated request id", store.event.RequestID)
	}
}

func TestCreateOrderRejectsTrailingJSON(t *testing.T) {
	t.Parallel()
	store := &recordingStore{}
	handler := NewHTTPHandler(NewService(store), slog.New(slog.NewTextHandler(io.Discard, nil))).Routes(http.NotFoundHandler())
	request := httptest.NewRequest(http.MethodPost, "/api/v1/orders", bytes.NewBufferString(`{} {}`))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
}
