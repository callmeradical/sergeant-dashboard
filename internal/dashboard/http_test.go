package dashboard_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/callmeradical/sergeant-dashboard/internal/dashboard"
)

type fixedSource struct{ state dashboard.State }

func (source fixedSource) Collect(context.Context) dashboard.State { return source.state }

type sourceFunc func(context.Context) dashboard.State

func (collect sourceFunc) Collect(ctx context.Context) dashboard.State { return collect(ctx) }

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

func TestStateAPIBoundsProbeConcurrencyAcrossRequests(t *testing.T) {
	root := t.TempDir()
	for index := 0; index < 8; index++ {
		worker := filepath.Join(root, fmt.Sprintf("task-%02d", index), "api")
		worktree := filepath.Join(root, fmt.Sprintf("worktree-%02d", index))
		mustMkdirAll(t, worker)
		mustMkdirAll(t, worktree)
		writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
		writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	}

	var active atomic.Int32
	var maximum atomic.Int32
	var total atomic.Int32
	runner := func(ctx context.Context, _ string, name string, _ ...string) ([]byte, error) {
		total.Add(1)
		now := active.Add(1)
		defer active.Add(-1)
		for {
			old := maximum.Load()
			if now <= old || maximum.CompareAndSwap(old, now) {
				break
			}
		}
		select {
		case <-time.After(100 * time.Millisecond):
			if name == "no-mistakes" {
				return []byte("review"), nil
			}
			return []byte(`{"url":"https://github.com/acme/api/pull/7","state":"OPEN"}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	handler := dashboard.NewHandler(dashboard.Collector{FleetRoot: root, Run: runner, ProbeTimeout: 150 * time.Millisecond})
	var requests sync.WaitGroup
	responses := make(chan *httptest.ResponseRecorder, 2)
	requests.Add(2)
	for range 2 {
		go func() {
			defer requests.Done()
			responses <- request(t, handler, http.MethodGet, "/sergeant/api/state")
		}()
	}
	requests.Wait()
	close(responses)

	if got := maximum.Load(); got > 16 {
		t.Fatalf("maximum active probes across requests = %d, want at most 16", got)
	}
	if got := total.Load(); got != 16 {
		t.Fatalf("probes across overlapping requests = %d, want one shared 16-probe collection", got)
	}
	for response := range responses {
		body := response.Body.String()
		if got := strings.Count(body, `"url":"https://github.com/acme/api/pull/7"`); got != 8 {
			t.Errorf("complete pull request projections = %d, want 8: %s", got, body)
		}
		if got := strings.Count(body, `"available":true`); got != 8 {
			t.Errorf("complete no-mistakes projections = %d, want 8: %s", got, body)
		}
	}
}

func TestStateAPILeaderCancellationDoesNotCancelSurvivingRequest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	source := sourceFunc(func(ctx context.Context) dashboard.State {
		calls.Add(1)
		once.Do(func() { close(started) })
		select {
		case <-release:
			return dashboard.State{Workers: []dashboard.Worker{{
				Task: "task", Project: "api", Health: "active",
				PullRequest: dashboard.PullRequest{URL: "https://github.com/acme/api/pull/7", Checks: []dashboard.Check{}},
			}}, Warnings: []string{}}
		case <-ctx.Done():
			return dashboard.State{Workers: []dashboard.Worker{}, Warnings: []string{}}
		}
	})
	handler := dashboard.NewHandler(source)

	leaderCtx, cancelLeader := context.WithCancel(t.Context())
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		request := httptest.NewRequest(http.MethodGet, "/sergeant/api/state", nil).WithContext(leaderCtx)
		handler.ServeHTTP(httptest.NewRecorder(), request)
	}()
	<-started

	followerDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		followerDone <- request(t, handler, http.MethodGet, "/sergeant/api/state")
	}()
	time.Sleep(25 * time.Millisecond)
	cancelLeader()
	time.Sleep(25 * time.Millisecond)
	close(release)

	follower := <-followerDone
	<-leaderDone
	if got := calls.Load(); got != 1 {
		t.Fatalf("collections = %d, want one shared collection", got)
	}
	if !strings.Contains(follower.Body.String(), `"url":"https://github.com/acme/api/pull/7"`) {
		t.Fatalf("surviving request received incomplete shared state: %s", follower.Body.String())
	}
}

func TestStateAPICancelsSharedCollectionAfterAllRequestsCancel(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	source := sourceFunc(func(ctx context.Context) dashboard.State {
		close(started)
		<-ctx.Done()
		close(canceled)
		return dashboard.State{Workers: []dashboard.Worker{}, Warnings: []string{}}
	})
	handler := dashboard.NewHandler(source)
	requestCtx, cancelRequest := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		request := httptest.NewRequest(http.MethodGet, "/sergeant/api/state", nil).WithContext(requestCtx)
		handler.ServeHTTP(httptest.NewRecorder(), request)
	}()
	<-started
	cancelRequest()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("shared collection continued after every request canceled")
	}
	<-done
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

func TestStateAPIAliasesAreNotRecoverableUnsaltedHashes(t *testing.T) {
	state := dashboard.State{Workers: []dashboard.Worker{{Task: "api", Project: "web", Health: "active"}}}
	response := request(t, dashboard.NewHandler(fixedSource{state: state}), http.MethodGet, "/sergeant/api/state")
	body := response.Body.String()
	for kind, value := range map[string]string{"task": "api", "project": "web"} {
		digest := sha256.Sum256([]byte(value))
		predictable := fmt.Sprintf("%s-%x", kind, digest[:6])
		if strings.Contains(body, predictable) {
			t.Errorf("state API exposed predictable alias %q", predictable)
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

func TestEmbeddedApplicationRendersBrowserBehavior(t *testing.T) {
	state := dashboard.State{
		CollectedAt: time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC),
		Workers: []dashboard.Worker{
			{
				Task: "raw-private-task", Project: "raw-private-project", Status: "in_progress", Health: "active", Agent: "opencode",
				Branch: "secret-branch", TDTask: "td-123", Worktree: "/secret/worktree",
				Message: dashboard.FileMetadata{Present: true, Summary: "secret message body"},
				PullRequest: dashboard.PullRequest{
					URL: "https://github.com/acme/widget/pull/7?token=secret", State: "OPEN",
					Checks: []dashboard.Check{{Name: "private check name", Conclusion: "SUCCESS"}},
				},
				NoMistakes: dashboard.ToolStatus{Available: true, Phase: "review"}, Graphify: dashboard.FileMetadata{Present: true, Summary: "ready"},
				OCInject: dashboard.AuditMetadata{ResponsePending: true},
			},
			{Task: "stale-task", Project: "stale-project", Status: "blocked", Health: "stale", TDTask: "invalid task"},
		},
		Warnings: []string{"secret source warning"},
	}
	valid := dashboard.NewHandler(fixedSource{state: state})
	empty := dashboard.NewHandler(fixedSource{state: dashboard.State{Workers: []dashboard.Worker{}, Warnings: []string{}}})
	assets := dashboard.NewHandler(fixedSource{})
	malformed := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/sergeant/api/state" {
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"workers":`))
			return
		}
		assets.ServeHTTP(writer, request)
	})
	type browserResponse struct {
		Status  int         `json:"status"`
		Headers http.Header `json:"headers"`
		Body    string      `json:"body"`
	}
	fixtures := make(map[string]browserResponse)
	for prefix, handler := range map[string]http.Handler{"/valid": valid, "/empty": empty, "/malformed": malformed} {
		for _, path := range []string{"/sergeant/", "/sergeant/app.css", "/sergeant/app.js", "/sergeant/api/state"} {
			response := request(t, handler, http.MethodGet, path)
			fixtures[prefix+path] = browserResponse{Status: response.Code, Headers: response.Header(), Body: base64.StdEncoding.EncodeToString(response.Body.Bytes())}
		}
	}
	fixtureJSON, err := json.Marshal(fixtures)
	if err != nil {
		t.Fatal(err)
	}
	browser := os.Getenv("CHROME_BIN")
	if browser == "" {
		for _, candidate := range []string{"google-chrome", "chromium", "chromium-browser"} {
			if path, lookErr := exec.LookPath(candidate); lookErr == nil {
				browser = path
				break
			}
		}
	}
	if browser == "" {
		t.Fatal("Chrome or Chromium is required for frontend behavior tests")
	}

	command := exec.Command("node", "testdata/frontend_test.mjs")
	command.Env = append(os.Environ(), "CHROME_BIN="+browser, "FRONTEND_FIXTURES="+base64.StdEncoding.EncodeToString(fixtureJSON))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("frontend behavior: %v: %s", err, output)
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

func TestServeStatusBindsCanonicalHTTPSURL(t *testing.T) {
	serveStatus := `{"Web":{"cleanthes.taila4fb6a.ts.net:443":{"Handlers":{"/sergeant":{"Proxy":"http://127.0.0.1:8992/sergeant"}}}}}`
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{name: "approved", url: "https://cleanthes.taila4fb6a.ts.net/sergeant/"},
		{name: "http", url: "http://cleanthes.taila4fb6a.ts.net/sergeant/", wantErr: true},
		{name: "wrong-host", url: "https://other-host.ts.net/sergeant/", wantErr: true},
		{name: "wrong-port", url: "https://cleanthes.taila4fb6a.ts.net:8443/sergeant/", wantErr: true},
		{name: "userinfo", url: "https://user@cleanthes.taila4fb6a.ts.net/sergeant/", wantErr: true},
		{name: "query", url: "https://cleanthes.taila4fb6a.ts.net/sergeant/?token=secret", wantErr: true},
		{name: "fragment", url: "https://cleanthes.taila4fb6a.ts.net/sergeant/#secret", wantErr: true},
		{name: "wrong-path", url: "https://cleanthes.taila4fb6a.ts.net/other/", wantErr: true},
		{name: "malformed", url: "://not-a-url", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := dashboard.ValidateServeURL(bytes.NewBufferString(serveStatus), test.url, "http://127.0.0.1:8992/sergeant")
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateServeURL(%q) error = %v, want error %t", test.url, err, test.wantErr)
			}
		})
	}
}

func request(t *testing.T, handler http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, path, nil))
	return recorder
}
