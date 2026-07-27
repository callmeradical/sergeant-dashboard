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
	if got := total.Load(); got != 24 {
		t.Fatalf("probes across overlapping requests = %d, want one shared 24-probe collection", got)
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

func TestStateAPIReturnsNoStoreServiceUnavailableWhenCollectionIsCanceled(t *testing.T) {
	source := sourceFunc(func(ctx context.Context) dashboard.State {
		<-ctx.Done()
		return dashboard.State{Workers: []dashboard.Worker{{Task: "stale-worker"}}, Warnings: []string{"private diagnostic"}}
	})
	handler := dashboard.NewHandler(source)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request := httptest.NewRequest(http.MethodGet, "/sergeant/api/state", nil).WithContext(ctx)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("state response = %d %q, want 503", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("state cache control = %q, want no-store", got)
	}
	if strings.Contains(response.Body.String(), "stale-worker") || strings.Contains(response.Body.String(), "private diagnostic") {
		t.Fatalf("degraded response exposed stale or private state: %q", response.Body.String())
	}
}

func TestStateAPIProjectsTrustedOperatorContextWithSecretsRedacted(t *testing.T) {
	state := dashboard.State{
		Workers: []dashboard.Worker{{
			Task: "build-dashboard-89e093", Project: "sergeant-dashboard", Repository: "callmeradical/sergeant-dashboard", Status: "needs_input: approve rollout", Health: "active", Agent: "opencode",
			Branch: "feat/trusted-dashboard", TDTask: "td-7ab86d", Worktree: "/srv/repos/sergeant-dashboard",
			Message:    dashboard.FileMetadata{Present: true, Summary: "Approval needed; token=opaque-api-secret"},
			Diagnostic: dashboard.FileMetadata{Present: true, Summary: "collector delayed; retry is safe"},
			Log:        dashboard.FileMetadata{Present: true, Summary: "validated 42 workers"},
			Handoff:    dashboard.FileMetadata{Present: true, Summary: "remaining: open PR"},
			PullRequest: dashboard.PullRequest{
				URL:   "https://github.com/acme/widget/pull/7?access_token=opaque-api-secret",
				State: "OPEN",
				Checks: []dashboard.Check{{
					Name: "integration tests", Status: "COMPLETED", Conclusion: "SUCCESS", State: "PENDING",
				}},
				Comments: []dashboard.Comment{{Author: "operator", Body: "Looks good; password=hunter2"}},
			},
			NoMistakes: dashboard.ToolStatus{Available: true, Phase: "review", Summary: "review passed"},
			Graphify:   dashboard.FileMetadata{Present: true, Summary: "god nodes: Collector"},
		}},
		Warnings: []string{"worker delayed; Authorization: Bearer opaque-warning-secret"},
	}

	response := request(t, dashboard.NewHandler(fixedSource{state: state}), http.MethodGet, "/sergeant/api/state")
	if response.Code != http.StatusOK {
		t.Fatalf("state response = %d %q", response.Code, response.Body.String())
	}
	body := response.Body.String()
	for _, forbidden := range []string{
		"opaque-api-secret", "hunter2", "opaque-warning-secret", "access_token",
	} {
		if strings.Contains(body, forbidden) {
			t.Errorf("state API exposed non-allowlisted value %q: %s", forbidden, body)
		}
	}
	for _, allowed := range []string{
		`"task":"build-dashboard-89e093"`, `"project":"sergeant-dashboard"`, `"status":"needs_input: approve rollout"`,
		`"repository":"callmeradical/sergeant-dashboard"`,
		`"branch":"feat/trusted-dashboard"`, `"tdTask":"td-7ab86d"`, `"worktree":"/srv/repos/sergeant-dashboard"`,
		`"summary":"Approval needed; token=[REDACTED]"`, `"summary":"collector delayed; retry is safe"`,
		`"summary":"validated 42 workers"`, `"summary":"remaining: open PR"`, `"name":"integration tests"`,
		`"body":"Looks good; password=[REDACTED]"`, `"summary":"review passed"`, `"summary":"god nodes: Collector"`,
		`"warnings":["worker delayed; Authorization: Bearer [REDACTED]"]`,
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

// TestStateAPIFiltersBeforeTruncating verifies that operational-set filtering
// happens before the maxProjectedWorkers safety truncation, so active workers
// past position 1000 in a large fleet are not silently dropped.
func TestStateAPIFiltersBeforeTruncating(t *testing.T) {
	// Build a state with 1002 workers: 1000 stale followed by 2 active.
	// If truncation precedes filtering the 2 active workers never appear.
	workers := make([]dashboard.Worker, 1002)
	for i := range 1000 {
		workers[i] = dashboard.Worker{Task: fmt.Sprintf("stale-%04d", i), Health: "stale", Status: "in_progress"}
	}
	workers[1000] = dashboard.Worker{Task: "active-tail-0", Health: "active", Status: "in_progress"}
	workers[1001] = dashboard.Worker{Task: "active-tail-1", Health: "active", Status: "in_progress"}

	state := dashboard.State{Workers: workers, Warnings: []string{}}
	response := request(t, dashboard.NewHandler(fixedSource{state: state}), http.MethodGet, "/sergeant/api/state")
	if response.Code != http.StatusOK {
		t.Fatalf("state response = %d", response.Code)
	}
	body := response.Body.String()
	if !strings.Contains(body, `"task":"active-tail-0"`) || !strings.Contains(body, `"task":"active-tail-1"`) {
		t.Errorf("active workers past position 1000 were dropped (truncated before filtered): %s", body[:min(200, len(body))])
	}
	if strings.Contains(body, `"health":"stale"`) {
		t.Errorf("stale workers leaked into operational set: %s", body[:min(200, len(body))])
	}
}

func TestStateAPIDoesNotWarnAtExactOperationalLimit(t *testing.T) {
	workers := make([]dashboard.Worker, 1000)
	for i := range workers {
		workers[i] = dashboard.Worker{Task: fmt.Sprintf("active-%04d", i), Health: "active", Status: "in_progress"}
	}

	response := request(t, dashboard.NewHandler(fixedSource{state: dashboard.State{Workers: workers, Warnings: []string{}}}), http.MethodGet, "/sergeant/api/state")
	if response.Code != http.StatusOK {
		t.Fatalf("state response = %d", response.Code)
	}
	if strings.Contains(response.Body.String(), "projection truncated at safety limit") {
		t.Fatalf("exact operational limit reported truncation: %s", response.Body.String())
	}
}

// TestStateAPIProjectsOnlyOperationalWorkersFromMixedHistory reproduces the
// 154-record live failure: the API should return only the operational set
// (verified active workers and actionable orphaned records), not the full
// retained fleet inventory.
func TestStateAPIProjectsOnlyOperationalWorkersFromMixedHistory(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	staleAfter := 30 * time.Minute

	mkWorker := func(task, project, status, pane string, worktree bool, age time.Duration) {
		dir := filepath.Join(root, task, project)
		mustMkdirAll(t, dir)
		writeFile(t, filepath.Join(dir, "status"), status+"\n")
		setModTime(t, filepath.Join(dir, "status"), now.Add(-age))
		if pane != "" {
			writeFile(t, filepath.Join(dir, "pane"), pane+"\n")
		}
		if worktree {
			wt := filepath.Join(root, task+"-wt")
			mustMkdirAll(t, wt)
			writeFile(t, filepath.Join(dir, "worktree"), wt+"\n")
		}
	}

	// 93 stale workers: in_progress, existing worktree, old status, dead/missing pane.
	// Includes one explicit reused-pane (prefix-colliding with an active worker's pane).
	mkWorker("stale-reused-pane", "api", "in_progress", "pane-live", true, staleAfter+time.Minute)
	for i := 1; i < 93; i++ {
		mkWorker(fmt.Sprintf("stale-%03d", i), "api", "in_progress", "", true, staleAfter+time.Minute)
	}

	// 1 active worker: in_progress, existing worktree, old status but LIVE pane.
	mkWorker("active-live", "api", "in_progress", "pane-live", true, staleAfter+time.Minute)

	// 43 complete workers: terminal status, worktree cleaned up.
	for i := 0; i < 40; i++ {
		mkWorker(fmt.Sprintf("complete-done-%03d", i), "api", "done", "", false, time.Hour)
	}
	for i := 0; i < 3; i++ {
		mkWorker(fmt.Sprintf("complete-failed-%03d", i), "api", "failed: command error", "", false, time.Hour)
	}

	// 7 orphaned workers: non-terminal status, worktree missing.
	for i := 0; i < 3; i++ {
		mkWorker(fmt.Sprintf("orphaned-needs-input-%03d", i), "api", "needs_input", "", false, time.Hour)
	}
	for i := 0; i < 2; i++ {
		mkWorker(fmt.Sprintf("orphaned-blocked-%03d", i), "api", "blocked", "", false, time.Hour)
	}
	for i := 0; i < 2; i++ {
		mkWorker(fmt.Sprintf("orphaned-explicit-%03d", i), "api", "orphaned", "", false, time.Hour)
	}

	// 10 unknown workers: invalid/malformed status.
	for i := 0; i < 10; i++ {
		mkWorker(fmt.Sprintf("unknown-%03d", i), "api", "invalid-status", "", false, time.Hour)
	}

	// Total: 93 stale + 1 active + 43 complete + 7 orphaned + 10 unknown = 154 workers.

	livePane := "pane-live"
	collector := dashboard.Collector{
		FleetRoot:  root,
		Now:        func() time.Time { return now },
		StaleAfter: staleAfter,
		InspectSupervisor: func(_ context.Context, pane, stateDir string) (bool, error) {
			// Only the worker whose stateDir ends in "active-live/api" has a live pane.
			return pane == livePane && strings.HasSuffix(stateDir, "active-live/api"), nil
		},
	}

	handler := dashboard.NewHandler(collector)
	response := request(t, handler, http.MethodGet, "/sergeant/api/state")
	if response.Code != http.StatusOK {
		t.Fatalf("state response = %d %s", response.Code, response.Body.String())
	}

	var state struct {
		Workers []struct {
			Task   string `json:"task"`
			Health string `json:"health"`
		} `json:"workers"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &state); err != nil {
		t.Fatalf("decode state: %v: %s", err, response.Body.String())
	}

	var active, orphaned int
	for _, w := range state.Workers {
		switch w.Health {
		case "active":
			active++
		case "orphaned":
			orphaned++
		default:
			t.Errorf("non-operational worker in response: task=%s health=%s", w.Task, w.Health)
		}
	}
	if got := len(state.Workers); got != 8 {
		t.Errorf("operational workers = %d, want 8 (1 active + 7 orphaned); got health dist active=%d orphaned=%d", got, active, orphaned)
	}
	if active != 1 {
		t.Errorf("active workers = %d, want 1 (pane-verified live supervisor)", active)
	}
	if orphaned != 7 {
		t.Errorf("orphaned workers = %d, want 7 (actionable non-terminal records)", orphaned)
	}
	// Stale-with-reused-pane must not be active: prefix-colliding pane does not verify identity.
	for _, w := range state.Workers {
		if w.Task == "stale-reused-pane" {
			t.Errorf("stale worker with reused pane appeared in operational set with health=%s", w.Health)
		}
	}
}

func TestEmbeddedApplicationRendersBrowserBehavior(t *testing.T) {
	state := dashboard.State{
		CollectedAt: time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC),
		Workers: []dashboard.Worker{
			{
				Task: "raw-private-task", Project: "raw-private-project", Status: "in_progress", Health: "active", Agent: "opencode",
				Branch: "secret-branch", TDTask: "td-123", Worktree: "/secret/worktree",
				Message:    dashboard.FileMetadata{Present: true, Summary: "approval needed; token=secret"},
				Diagnostic: dashboard.FileMetadata{Present: true, Summary: "worker recovered"},
				Log:        dashboard.FileMetadata{Present: true, Summary: "tests passed"},
				Handoff:    dashboard.FileMetadata{Present: true, Summary: "remaining: open PR"},
				PullRequest: dashboard.PullRequest{
					URL: "https://github.com/acme/widget/pull/7?token=secret", State: "OPEN", Status: "available truncated",
					Checks:   []dashboard.Check{{Name: "private check name", Conclusion: "SUCCESS"}},
					Comments: []dashboard.Comment{{Author: "reviewer", Body: "approved", URL: "https://github.com/acme/widget/pull/7#issuecomment-1"}},
				},
				NoMistakes: dashboard.ToolStatus{Available: true, Phase: "review", Summary: "review passed"}, Graphify: dashboard.FileMetadata{Present: true, Summary: "Collector connects fleet state"},
				OCInject: dashboard.AuditMetadata{ResponsePending: true},
			},
			{Task: "orphaned-task", Project: "orphaned-project", Status: "blocked", Health: "orphaned", TDTask: "invalid task", NoMistakes: dashboard.ToolStatus{Status: "unavailable"}, Graphify: dashboard.FileMetadata{Status: "missing"}},
		},
		Warnings: []string{"source delayed token=secret"},
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
