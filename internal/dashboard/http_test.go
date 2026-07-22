package dashboard_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/callmeradical/sergeant-dashboard/internal/dashboard"
)

type fixedSource struct{ state dashboard.State }

func (source fixedSource) Collect(context.Context) dashboard.State { return source.state }

func TestHandlerServesHealthStateAndEmbeddedApplication(t *testing.T) {
	state := dashboard.State{
		CollectedAt: time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC),
		Workers:     []dashboard.Worker{{Task: "task-1", Project: "api", Health: "active"}},
		Warnings:    []string{},
	}
	handler := dashboard.NewHandler(fixedSource{state: state})

	health := request(t, handler, http.MethodGet, "/healthz")
	if health.Code != http.StatusOK || health.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("health response = %d %q", health.Code, health.Body.String())
	}
	var healthBody map[string]string
	if err := json.Unmarshal(health.Body.Bytes(), &healthBody); err != nil || healthBody["status"] != "ok" {
		t.Fatalf("health body = %q (%v)", health.Body.String(), err)
	}

	api := request(t, handler, http.MethodGet, "/sergeant/api/state")
	if api.Code != http.StatusOK || !strings.Contains(api.Body.String(), `"task":"task-1"`) {
		t.Fatalf("state response = %d %q", api.Code, api.Body.String())
	}
	if api.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("state cache control = %q", api.Header().Get("Cache-Control"))
	}

	app := request(t, handler, http.MethodGet, "/sergeant/")
	if app.Code != http.StatusOK || !strings.Contains(app.Body.String(), "Fleet command") {
		t.Fatalf("app response = %d %q", app.Code, app.Body.String())
	}
	if app.Header().Get("Content-Security-Policy") == "" {
		t.Fatal("application response lacks a content security policy")
	}
	script := request(t, handler, http.MethodGet, "/sergeant/app.js")
	if strings.Contains(script.Body.String(), "worker.message?.summary") {
		t.Fatal("embedded application can render worker message bodies")
	}
	for _, required := range []string{"worker.message?.present", "worker.pullRequest?.checks", "worker.noMistakes?.available", "worker.noMistakes?.phase", "worker.graphify?.summary", "worker.graphify?.updatedAt"} {
		if !strings.Contains(script.Body.String(), required) {
			t.Errorf("embedded application lacks %q", required)
		}
	}
}

func TestHandlerIsReadOnly(t *testing.T) {
	handler := dashboard.NewHandler(fixedSource{state: dashboard.State{Workers: []dashboard.Worker{}, Warnings: []string{}}})
	for _, path := range []string{"/healthz", "/sergeant/api/state", "/sergeant/"} {
		response := request(t, handler, http.MethodPost, path)
		if response.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s = %d, want 405", path, response.Code)
		}
	}
}

func request(t *testing.T, handler http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, path, nil))
	return recorder
}
