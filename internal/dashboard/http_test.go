package dashboard_test

import (
	"bytes"
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
	if api.Code != http.StatusOK || !strings.Contains(api.Body.String(), `"health":"active"`) {
		t.Fatalf("state response = %d %q", api.Code, api.Body.String())
	}
	if api.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("state cache control = %q", api.Header().Get("Cache-Control"))
	}

	app := request(t, handler, http.MethodGet, "/sergeant/")
	if app.Code != http.StatusOK || !strings.Contains(app.Body.String(), "Fleet command") {
		t.Fatalf("app response = %d %q", app.Code, app.Body.String())
	}
	if policy := app.Header().Get("Content-Security-Policy"); !strings.Contains(policy, "img-src 'self' data:") {
		t.Fatalf("application content security policy blocks its embedded image: %q", policy)
	}
	script := request(t, handler, http.MethodGet, "/sergeant/app.js")
	if strings.Contains(script.Body.String(), "worker.message?.summary") {
		t.Fatal("embedded application can render worker message bodies")
	}
	for _, required := range []string{"worker.message?.present", "worker.pullRequest?.checks", "worker.noMistakes?.available", "worker.noMistakes?.phase", "worker.graphify?.status", "worker.graphify?.updatedAt"} {
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

func TestStateAPIProjectsOnlyAllowlistedMetadata(t *testing.T) {
	state := dashboard.State{
		Workers: []dashboard.Worker{{
			Task: "injected-prompt-body-89e093", Project: "private-repository", Status: "in_progress", Health: "active", Agent: "opencode",
			Branch: "opaque-branch-secret", TDTask: "td-7ab86d", Worktree: "/private/repository/path",
			Message: dashboard.FileMetadata{Present: true, Summary: "raw injected message body"},
			PullRequest: dashboard.PullRequest{
				URL:   "https://github.com/acme/widget/pull/7?access_token=opaque-api-secret",
				State: "OPEN",
				Checks: []dashboard.Check{{
					Name: "credential copied from API", Status: "COMPLETED", Conclusion: "SUCCESS", State: "PENDING",
				}},
			},
			NoMistakes: dashboard.ToolStatus{Available: true, Phase: "review"},
			Graphify:   dashboard.FileMetadata{Present: true, Summary: "ready"},
		}},
		Warnings: []string{"worker injected-prompt-body-89e093 has credential opaque-warning-secret"},
	}

	response := request(t, dashboard.NewHandler(fixedSource{state: state}), http.MethodGet, "/sergeant/api/state")
	if response.Code != http.StatusOK {
		t.Fatalf("state response = %d %q", response.Code, response.Body.String())
	}
	body := response.Body.String()
	for _, forbidden := range []string{
		"injected-prompt-body", "private-repository", "opaque-branch-secret", "/private/repository/path",
		"raw injected message body", "access_token", "opaque-api-secret", "credential copied from API", "opaque-warning-secret",
		`"branch"`, `"worktree"`, `"summary"`,
	} {
		if strings.Contains(body, forbidden) {
			t.Errorf("state API exposed non-allowlisted value %q: %s", forbidden, body)
		}
	}
	for _, allowed := range []string{
		`"status":"in_progress"`, `"health":"active"`, `"agent":"opencode"`, `"tdTask":"td-7ab86d"`,
		`"url":"https://github.com/acme/widget/pull/7"`, `"state":"OPEN"`, `"status":"COMPLETED"`,
		`"conclusion":"SUCCESS"`, `"state":"PENDING"`, `"phase":"review"`, `"graphify":{"present":true`,
	} {
		if !strings.Contains(body, allowed) {
			t.Errorf("state API omitted allowlisted value %q: %s", allowed, body)
		}
	}
}

func TestStateAPIProjectsBlockedLifecycleStatus(t *testing.T) {
	state := dashboard.State{Workers: []dashboard.Worker{{Status: "blocked", Health: "active"}}}

	response := request(t, dashboard.NewHandler(fixedSource{state: state}), http.MethodGet, "/sergeant/api/state")
	if !strings.Contains(response.Body.String(), `"status":"blocked"`) {
		t.Fatalf("state API omitted blocked lifecycle status: %s", response.Body.String())
	}
}

func TestEmbeddedApplicationLinksOnlyValidTDTaskIDs(t *testing.T) {
	script := request(t, dashboard.NewHandler(fixedSource{}), http.MethodGet, "/sergeant/app.js").Body.String()
	for _, required := range []string{"tdTaskLink", "/api/issues/", "encodeURIComponent", "^td-[A-Za-z0-9]+$"} {
		if !strings.Contains(script, required) {
			t.Errorf("embedded application lacks safe td link behavior %q", required)
		}
	}
}

func TestServeStatusRejectsMalformedAndTrailingInput(t *testing.T) {
	for name, input := range map[string]string{
		"malformed": `{"Web":`,
		"trailing":  `{"Web":{"host:443":{"Handlers":{"/sergeant":{"Proxy":"http://127.0.0.1:8992/sergeant"}}}}} {"injected":"credential"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := dashboard.ValidateServeStatus(bytes.NewBufferString(input), "cleanthes.taila4fb6a.ts.net:443", "/sergeant", "http://127.0.0.1:8992/sergeant"); err == nil {
				t.Fatalf("ValidateServeStatus accepted %s input", name)
			}
		})
	}
}

func TestServeStatusRequiresExpectedHTTPSHost(t *testing.T) {
	input := `{"Web":{"other-host:443":{"Handlers":{"/sergeant":{"Proxy":"http://127.0.0.1:8992/sergeant"}}}}}`
	if err := dashboard.ValidateServeStatus(bytes.NewBufferString(input), "cleanthes.taila4fb6a.ts.net:443", "/sergeant", "http://127.0.0.1:8992/sergeant"); err == nil {
		t.Fatal("ValidateServeStatus accepted a route on an unrelated HTTPS host")
	}
}

func request(t *testing.T, handler http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, path, nil))
	return recorder
}
