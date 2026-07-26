package dashboard_test

import (
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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/callmeradical/sergeant-dashboard/internal/dashboard"
)

type fixedSource struct{ state dashboard.State }

func (source fixedSource) Collect(context.Context) dashboard.State { return source.state }

type fixedDetailSource struct {
	state  dashboard.State
	worker dashboard.Worker
}

type canceledDetailSource struct{ fixedSource }

func (source canceledDetailSource) CollectDetail(ctx context.Context, _, _ string) (dashboard.Worker, bool) {
	<-ctx.Done()
	return dashboard.Worker{}, false
}

func (source fixedDetailSource) Collect(context.Context) dashboard.State { return source.state }
func (source fixedDetailSource) CollectDetail(_ context.Context, task, repository string) (dashboard.Worker, bool) {
	return source.worker, task == source.worker.Task && repository == source.worker.Repository
}

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
	for _, path := range []string{"/healthz", "/sergeant/api/state", "/sergeant/api/workers/task/api", "/sergeant/"} {
		response := request(t, handler, http.MethodPost, path)
		if response.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s = %d, want 405", path, response.Code)
		}
	}
}

func TestDetailAPIReturnsServiceUnavailableWhenCollectionIsCanceled(t *testing.T) {
	handler := dashboard.NewHandler(canceledDetailSource{})
	requestContext, cancel := context.WithCancel(t.Context())
	cancel()
	httpRequest := httptest.NewRequest(http.MethodGet, "/sergeant/api/workers/task/api", nil).WithContext(requestContext)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httpRequest)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("detail response = %d %q, want 503", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("detail cache control = %q, want no-store", got)
	}
}

func TestStateAPIDoesNotRunDetailProbesAcrossRequests(t *testing.T) {
	root := t.TempDir()
	for index := 0; index < 8; index++ {
		worker := filepath.Join(root, fmt.Sprintf("task-%02d", index), "api")
		worktree := filepath.Join(root, fmt.Sprintf("worktree-%02d", index))
		mustMkdirAll(t, worker)
		mustMkdirAll(t, worktree)
		writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
		writeFile(t, filepath.Join(worker, "pane"), fmt.Sprintf("pane-%02d\n", index))
		writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	}

	var active atomic.Int32
	var maximum atomic.Int32
	var total atomic.Int32
	var detailProbes atomic.Int32
	runner := func(ctx context.Context, _ string, name string, args ...string) ([]byte, error) {
		total.Add(1)
		if name != "gh" || !slices.Equal(args, []string{"pr", "view", "--json", "state"}) {
			detailProbes.Add(1)
		}
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

	handler := dashboard.NewHandler(dashboard.Collector{
		FleetRoot: root, Run: runner, ProbeTimeout: 150 * time.Millisecond,
		InspectSupervisor: func(context.Context, string, string) (bool, error) { return true, nil },
	})
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
	if got := total.Load(); got != 8 {
		t.Fatalf("summary probes across overlapping requests = %d, want one shared 8-worker collection", got)
	}
	if got := detailProbes.Load(); got != 0 {
		t.Fatalf("detail probes across summary requests = %d, want 0", got)
	}
	for response := range responses {
		body := response.Body.String()
		if got := strings.Count(body, `"health":"active"`); got != 8 {
			t.Errorf("complete summary projections = %d, want 8: %s", got, body)
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
	if !strings.Contains(follower.Body.String(), `"task":"task"`) {
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

func TestDetailAPIProjectsTrustedOperatorContextWithSecretsRedacted(t *testing.T) {
	worker := dashboard.Worker{
		Task: "build-dashboard-89e093", Project: "sergeant-dashboard", Repository: "sergeant-dashboard", Status: "needs_input: approve rollout", Health: "active", Agent: "opencode",
		Branch: "feat/trusted-dashboard", TDTask: "td-7ab86d", Worktree: "/srv/repos/sergeant-dashboard",
		Message:    dashboard.FileMetadata{Present: true, Summary: "Approval needed; token=opaque-api-secret"},
		Diagnostic: dashboard.FileMetadata{Present: true, Summary: "collector delayed; retry is safe"},
		Log:        dashboard.FileMetadata{Present: true, Summary: "validated 42 workers"},
		Handoff:    dashboard.FileMetadata{Present: true, Summary: "remaining: open PR"},
		PullRequest: dashboard.PullRequest{
			URL:   "https://github.com/acme/widget/pull/7?access_token=opaque-api-secret",
			State: "OPEN",
			Checks: []dashboard.Check{{
				Name: "integration tests", Context: "legacy-ci", Status: "COMPLETED", Conclusion: "SUCCESS", State: "PENDING",
			}},
			Comments: []dashboard.Comment{{Author: "operator", Body: "Looks good; password=hunter2"}},
		},
		NoMistakes: dashboard.ToolStatus{Available: true, Phase: "review", Summary: "review passed"},
		Graphify:   dashboard.FileMetadata{Present: true, Summary: "god nodes: Collector"},
	}
	state := dashboard.State{
		Workers:  []dashboard.Worker{worker},
		Warnings: []string{"worker delayed; Authorization: Bearer opaque-warning-secret"},
	}

	response := request(t, dashboard.NewHandler(fixedDetailSource{state: state, worker: worker}), http.MethodGet, "/sergeant/api/workers/build-dashboard-89e093/sergeant-dashboard")
	if response.Code != http.StatusOK {
		t.Fatalf("state response = %d %q", response.Code, response.Body.String())
	}
	body := response.Body.String()
	for _, forbidden := range []string{
		"opaque-api-secret", "hunter2", "access_token",
	} {
		if strings.Contains(body, forbidden) {
			t.Errorf("state API exposed non-allowlisted value %q: %s", forbidden, body)
		}
	}
	for _, allowed := range []string{
		`"task":"build-dashboard-89e093"`, `"project":"sergeant-dashboard"`, `"status":"needs_input: approve rollout"`,
		`"repository":"sergeant-dashboard"`,
		`"branch":"feat/trusted-dashboard"`, `"tdTask":"td-7ab86d"`, `"worktree":"/srv/repos/sergeant-dashboard"`,
		`"summary":"Approval needed; token=[REDACTED]"`, `"summary":"collector delayed; retry is safe"`,
		`"summary":"validated 42 workers"`, `"summary":"remaining: open PR"`, `"name":"integration tests"`, `"context":"legacy-ci"`,
		`"body":"Looks good; password=[REDACTED]"`, `"summary":"review passed"`, `"summary":"god nodes: Collector"`,
	} {
		if !strings.Contains(body, allowed) {
			t.Errorf("state API omitted allowlisted value %q: %s", allowed, body)
		}
	}
}

func TestStateSummaryDoesNotPreloadWorkerDetail(t *testing.T) {
	root := t.TempDir()
	configRoot := t.TempDir()
	task := filepath.Join(root, "task-1")
	worker := filepath.Join(task, "api")
	worktree := filepath.Join(root, "worktree")
	mustMkdirAll(t, worker)
	mustMkdirAll(t, worktree)
	writeFile(t, filepath.Join(task, "brief.md"), "Project: operator-suite\nBrief:   Repair production API\n")
	writeFile(t, filepath.Join(configRoot, "operator-suite.yaml"), "name: operator-suite\nrepos:\n  - name: api\n    path: /srv/api\n")
	writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
	writeFile(t, filepath.Join(worker, "branch"), "feat/private-branch\n")
	writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	writeFile(t, filepath.Join(worker, "message"), "approval needed token=private-secret\n")

	var probes atomic.Int32
	var detailProbes atomic.Int32
	runner := func(_ context.Context, _ string, name string, args ...string) ([]byte, error) {
		probes.Add(1)
		if name != "gh" || !slices.Equal(args, []string{"pr", "view", "--json", "state"}) {
			detailProbes.Add(1)
		}
		switch name {
		case "gh":
			return []byte(`{"url":"https://github.com/acme/api/pull/7","state":"OPEN","statusCheckRollup":[{"context":"legacy-ci","state":"SUCCESS"}]}`), nil
		case "no-mistakes":
			return []byte("review passed"), nil
		default:
			return nil, fmt.Errorf("unexpected probe %s", name)
		}
	}
	handler := dashboard.NewHandler(dashboard.Collector{FleetRoot: root, ConfigRoot: configRoot, Run: runner})

	summary := request(t, handler, http.MethodGet, "/sergeant/api/state")
	if got := probes.Load(); got != 1 {
		t.Fatalf("summary request executed %d summary probes, want 1", got)
	}
	if got := detailProbes.Load(); got != 0 {
		t.Fatalf("summary request executed %d detail probes", got)
	}
	for _, expected := range []string{`"project":"operator-suite"`, `"repository":"api"`, `"title":"Repair production API"`, `"pullRequestState":"OPEN"`} {
		if !strings.Contains(summary.Body.String(), expected) {
			t.Errorf("summary omitted %s: %s", expected, summary.Body.String())
		}
	}
	for _, forbidden := range []string{"feat/private-branch", "approval needed", "private-secret", `"checks"`, `"message"`, `"worktree"`} {
		if strings.Contains(summary.Body.String(), forbidden) {
			t.Errorf("summary preloaded detail %q: %s", forbidden, summary.Body.String())
		}
	}

	detail := request(t, handler, http.MethodGet, "/sergeant/api/workers/task-1/api")
	if detail.Code != http.StatusOK {
		t.Fatalf("detail response = %d %s", detail.Code, detail.Body.String())
	}
	for _, expected := range []string{"feat/private-branch", "approval needed token=[REDACTED]", `"context":"legacy-ci"`} {
		if !strings.Contains(detail.Body.String(), expected) {
			t.Errorf("detail omitted %q: %s", expected, detail.Body.String())
		}
	}
	if got := probes.Load(); got == 0 {
		t.Fatal("detail request did not execute on-demand probes")
	}
}

func TestStateSummaryIncludesCanonicalPullRequestStateWithoutDetail(t *testing.T) {
	state := dashboard.State{Workers: []dashboard.Worker{{
		Task: "task", Project: "operator-suite", Repository: "api", Health: "active",
		PullRequest: dashboard.PullRequest{State: "OPEN", Checks: []dashboard.Check{{Name: "private check"}}},
	}}}
	response := request(t, dashboard.NewHandler(fixedSource{state: state}), http.MethodGet, "/sergeant/api/state")
	if response.Code != http.StatusOK {
		t.Fatalf("state response = %d %q", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"pullRequestState":"OPEN"`) {
		t.Fatalf("summary omitted canonical PR state: %s", response.Body.String())
	}
	if strings.Contains(response.Body.String(), "private check") || strings.Contains(response.Body.String(), `"checks"`) {
		t.Fatalf("summary exposed PR detail: %s", response.Body.String())
	}
}

func TestStateAggregatesWarningsWithoutPreloadingAffectedWorkers(t *testing.T) {
	warnings := make([]string, 0, 205)
	for index := range 8 {
		warnings = append(warnings, fmt.Sprintf("worker private-task-%d/api has invalid status", index))
	}
	for index := range 197 {
		warnings = append(warnings, fmt.Sprintf("worker private-task-%d/api probe unavailable token=private", index))
	}
	handler := dashboard.NewHandler(fixedSource{state: dashboard.State{Warnings: warnings}})

	state := request(t, handler, http.MethodGet, "/sergeant/api/state")
	if state.Code != http.StatusOK {
		t.Fatalf("state response = %d %s", state.Code, state.Body.String())
	}
	for _, expected := range []string{`"category":"invalid-status"`, `"count":8`, `"label":"8 invalid fleet statuses"`, `"severity":"warning"`} {
		if !strings.Contains(state.Body.String(), expected) {
			t.Errorf("warning summary omitted %s: %s", expected, state.Body.String())
		}
	}
	if strings.Contains(state.Body.String(), "private-task") || strings.Contains(state.Body.String(), "private") {
		t.Fatalf("state preloaded warning detail: %s", state.Body.String())
	}

	details := request(t, handler, http.MethodGet, "/sergeant/api/diagnostics")
	if details.Code != http.StatusOK {
		t.Fatalf("diagnostics response = %d %s", details.Code, details.Body.String())
	}
	for _, expected := range []string{`"category":"invalid-status"`, `"items"`, `"truncated":true`, `token=[REDACTED]`} {
		if !strings.Contains(details.Body.String(), expected) {
			t.Errorf("warning detail omitted %s: %s", expected, details.Body.String())
		}
	}
	if strings.Count(details.Body.String(), "private-task") > 100 || strings.Contains(details.Body.String(), "token=private") {
		t.Fatalf("warning details were unbounded or unredacted: %s", details.Body.String())
	}
}

func TestStateCategorizesFleetDiagnosticsByHighestSeverity(t *testing.T) {
	state := dashboard.State{Warnings: []string{
		"fleet collection truncated at safety limit",
		"task api corrupt source",
		"worker api probe failed",
		"fleet collection canceled at deadline",
	}}
	response := request(t, dashboard.NewHandler(fixedSource{state: state}), http.MethodGet, "/sergeant/api/state")
	for _, expected := range []string{`"category":"truncation"`, `"category":"source"`, `"category":"probe-failure"`, `"category":"collection-deadline"`, `"highestSeverity":"error"`} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Errorf("diagnostic categories omitted %s: %s", expected, response.Body.String())
		}
	}
}

func TestStateTreatsUnavailableFleetSourceAsError(t *testing.T) {
	state := dashboard.State{Warnings: []string{"fleet source unavailable"}}
	response := request(t, dashboard.NewHandler(fixedSource{state: state}), http.MethodGet, "/sergeant/api/state")
	if !strings.Contains(response.Body.String(), `"highestSeverity":"error"`) {
		t.Fatalf("unavailable fleet source severity = %s, want error", response.Body.String())
	}
}

func TestUnavailableConfiguredIdentityStillOpensTruthfulDetail(t *testing.T) {
	root := t.TempDir()
	task := filepath.Join(root, "task")
	worker := filepath.Join(task, "opaque-repository")
	mustMkdirAll(t, worker)
	writeFile(t, filepath.Join(task, "brief.md"), "Project: missing-project\nBrief: Investigate worker\n")
	writeFile(t, filepath.Join(worker, "status"), "done\n")
	handler := dashboard.NewHandler(dashboard.Collector{FleetRoot: root, ConfigRoot: t.TempDir()})

	summary := request(t, handler, http.MethodGet, "/sergeant/api/state")
	for _, expected := range []string{`"project":"unavailable"`, `"repository":"unavailable"`, `"detailKey":"opaque-repository"`} {
		if !strings.Contains(summary.Body.String(), expected) {
			t.Errorf("summary omitted %s: %s", expected, summary.Body.String())
		}
	}
	detail := request(t, handler, http.MethodGet, "/sergeant/api/workers/task/opaque-repository")
	if detail.Code != http.StatusOK || !strings.Contains(detail.Body.String(), `"project":"unavailable"`) {
		t.Fatalf("unavailable identity detail = %d %s, want truthful detail", detail.Code, detail.Body.String())
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
				Task: "raw-private-task", Title: "Repair dashboard", Project: "operator-suite", Repository: "dashboard", Status: "in_progress", Health: "active", Agent: "opencode",
				Branch: "secret-branch", TDTask: "td-123", Worktree: "/secret/worktree",
				Message:    dashboard.FileMetadata{Present: true, Summary: "approval needed; token=secret"},
				Diagnostic: dashboard.FileMetadata{Present: true, Summary: "worker recovered"},
				Log:        dashboard.FileMetadata{Present: true, Summary: "tests passed"},
				Handoff:    dashboard.FileMetadata{Present: true, Summary: "remaining: open PR"},
				PullRequest: dashboard.PullRequest{
					URL: "https://github.com/acme/widget/pull/7?token=secret", State: "OPEN", Status: "available truncated",
					Checks:   []dashboard.Check{{Name: "private check name", Context: "legacy-ci", Conclusion: "SUCCESS"}},
					Comments: []dashboard.Comment{{Author: "reviewer", Body: "approved", URL: "https://github.com/acme/widget/pull/7#issuecomment-1"}},
				},
				NoMistakes: dashboard.ToolStatus{Available: true, Phase: "review", Summary: "review passed"}, Graphify: dashboard.FileMetadata{Present: true, Summary: "Collector connects fleet state"},
				OCInject: dashboard.AuditMetadata{ResponsePending: true},
			},
			{Task: "stale-task", Title: "Investigate queue", Project: "operator-suite", Repository: "worker", Status: "blocked", Health: "stale", TDTask: "invalid task", NoMistakes: dashboard.ToolStatus{Status: "unavailable"}, Graphify: dashboard.FileMetadata{Status: "missing"}},
		},
		Warnings: []string{"summary probe failed", "source delayed token=secret"},
	}
	valid := dashboard.NewHandler(fixedDetailSource{state: state, worker: state.Workers[0]})
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
		for _, path := range []string{"/sergeant/", "/sergeant/app.css", "/sergeant/app.js", "/sergeant/api/state", "/sergeant/api/diagnostics", "/sergeant/api/workers/raw-private-task/dashboard"} {
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
	fixturePath := filepath.Join(t.TempDir(), "frontend-fixtures.json")
	if err := os.WriteFile(fixturePath, fixtureJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	command.Env = append(os.Environ(), "CHROME_BIN="+browser, "FRONTEND_FIXTURES_FILE="+fixturePath)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("frontend behavior: %v: %s", err, output)
	}
}

func request(t *testing.T, handler http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, path, nil))
	return recorder
}
