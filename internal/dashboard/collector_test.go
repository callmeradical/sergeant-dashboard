package dashboard_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/callmeradical/sergeant-dashboard/internal/dashboard"
)

func TestCollectorProbesWorkersConcurrently(t *testing.T) {
	root := t.TempDir()
	for _, task := range []string{"task-a", "task-b"} {
		worker := filepath.Join(root, task, "api")
		worktree := filepath.Join(root, task+"-worktree")
		mustMkdirAll(t, worker)
		mustMkdirAll(t, worktree)
		writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
		writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	}
	var current atomic.Int32
	var maximum atomic.Int32
	release := make(chan struct{})
	var once sync.Once
	runner := func(ctx context.Context, _ string, _ string, _ ...string) ([]byte, error) {
		now := current.Add(1)
		defer current.Add(-1)
		for {
			old := maximum.Load()
			if now <= old || maximum.CompareAndSwap(old, now) {
				break
			}
		}
		if now >= 2 {
			once.Do(func() { close(release) })
		}
		select {
		case <-release:
			return []byte(`{}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	dashboard.Collector{FleetRoot: root, Run: runner, ProbeTimeout: 100 * time.Millisecond}.Collect(t.Context())
	if maximum.Load() < 2 {
		t.Fatalf("maximum concurrent probes = %d, want at least 2", maximum.Load())
	}
}

func TestCollectorUsesAnIndependentTimeoutForEachProbe(t *testing.T) {
	root := t.TempDir()
	worker := filepath.Join(root, "task-a", "api")
	worktree := filepath.Join(root, "worktree")
	mustMkdirAll(t, worker)
	mustMkdirAll(t, worktree)
	writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
	writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")

	runner := func(ctx context.Context, _ string, name string, _ ...string) ([]byte, error) {
		if name == "gh" {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return []byte("run detected"), nil
	}

	state := dashboard.Collector{FleetRoot: root, Run: runner, ProbeTimeout: time.Millisecond}.Collect(t.Context())
	if !state.Workers[0].NoMistakes.Available {
		t.Fatal("a timed-out GitHub probe exhausted the no-mistakes probe timeout")
	}
}

func TestCollectorBoundsDegradedFleetLatency(t *testing.T) {
	root := t.TempDir()
	for index := 0; index < 17; index++ {
		worker := filepath.Join(root, fmt.Sprintf("task-%02d", index), "api")
		worktree := filepath.Join(root, fmt.Sprintf("worktree-%02d", index))
		mustMkdirAll(t, worker)
		mustMkdirAll(t, worktree)
		writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
		writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	}

	var active atomic.Int32
	runner := func(ctx context.Context, _ string, _ string, _ ...string) ([]byte, error) {
		active.Add(1)
		defer active.Add(-1)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	probeTimeout := 100 * time.Millisecond
	started := time.Now()
	state := dashboard.Collector{FleetRoot: root, Run: runner, ProbeTimeout: probeTimeout}.Collect(t.Context())
	elapsed := time.Since(started)

	if len(state.Workers) != 17 {
		t.Fatalf("workers = %d, want complete 17-worker projection", len(state.Workers))
	}
	if elapsed >= 4*probeTimeout {
		t.Fatalf("degraded collection took %v, want less than %v", elapsed, 4*probeTimeout)
	}
	if got := active.Load(); got != 0 {
		t.Fatalf("active probes after collection = %d, want 0", got)
	}
}

func TestCollectorEnrichesWorkerWithReadOnlyDeliveryMetadata(t *testing.T) {
	root := t.TempDir()
	worker := filepath.Join(root, "task-123", "api")
	worktree := filepath.Join(root, "worktree")
	mustMkdirAll(t, worker)
	mustMkdirAll(t, filepath.Join(worktree, "graphify-out"))
	writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
	writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	writeFile(t, filepath.Join(worker, "response_id"), "opaque-secret-id\n")
	writeFile(t, filepath.Join(worktree, "graphify-out", "GRAPH_REPORT.md"), "# report\n")
	writeFile(t, filepath.Join(worktree, "graphify-out", ".needs_update"), "")

	runner := func(_ context.Context, dir, name string, args ...string) ([]byte, error) {
		if dir != worktree {
			t.Fatalf("probe dir = %q, want %q", dir, worktree)
		}
		switch name {
		case "gh":
			return []byte(`{"url":"https://github.com/acme/api/pull/7","state":"OPEN","statusCheckRollup":[{"name":"test","status":"COMPLETED","conclusion":"SUCCESS"},{"context":"legacy-ci","state":"PENDING"}]}`), nil
		case "no-mistakes":
			return []byte("run 42 review running token=unrecognized-secret\n"), nil
		default:
			t.Fatalf("unexpected probe: %s %v", name, args)
			return nil, nil
		}
	}

	state := dashboard.Collector{FleetRoot: root, Run: runner}.Collect(t.Context())
	got := state.Workers[0]
	if got.PullRequest.URL != "https://github.com/acme/api/pull/7" || got.PullRequest.Checks[0].Conclusion != "SUCCESS" || got.PullRequest.Checks[1].State != "PENDING" {
		t.Fatalf("pull request = %#v", got.PullRequest)
	}
	if !got.NoMistakes.Available || got.NoMistakes.Phase != "review" {
		t.Fatalf("no-mistakes = %#v", got.NoMistakes)
	}
	if !got.Graphify.Present || got.Graphify.Summary != "update pending" || !got.OCInject.ResponsePending {
		t.Fatalf("graphify/oc-inject = %#v / %#v", got.Graphify, got.OCInject)
	}
	serialized := dashboard.MustJSON(state)
	if strings.Contains(serialized, "opaque-secret-id") || strings.Contains(serialized, "unrecognized-secret") {
		t.Fatal("opaque source data was exposed")
	}
}

func TestRedactMetadataIsDeterministicAndRemovesSecrets(t *testing.T) {
	input := map[string]any{
		"event":         "inject",
		"token":         "ghp_abcdefghijklmnopqrstuvwxyz0123456789",
		"Authorization": "Bearer super-secret",
		"detail":        "password=hunter2 safe-tail",
		"nested":        map[string]any{"prompt": "do not expose", "count": float64(2)},
	}
	want := `{"Authorization":"[REDACTED]","detail":"password=[REDACTED] safe-tail","event":"inject","nested":{"count":2,"prompt":"[REDACTED]"},"token":"[REDACTED]"}`
	first := dashboard.MustJSON(dashboard.RedactMetadata(input))
	second := dashboard.MustJSON(dashboard.RedactMetadata(input))
	if first != want || second != want {
		t.Fatalf("redacted metadata = %s / %s, want %s", first, second, want)
	}
}

func TestCollectorProjectsFleetWithoutReadingSensitiveBodies(t *testing.T) {
	root := t.TempDir()
	worker := filepath.Join(root, "task-123", "api")
	worktree := filepath.Join(root, "worktree")
	mustMkdirAll(t, worker)
	mustMkdirAll(t, worktree)
	writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
	writeFile(t, filepath.Join(worker, "agent"), "opencode\n")
	writeFile(t, filepath.Join(worker, "branch"), "feat/dashboard\n")
	writeFile(t, filepath.Join(worker, "td_task"), "td-123\n")
	writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	writeFile(t, filepath.Join(worker, "message"), "Approval required; password=hunter2\n")
	writeFile(t, filepath.Join(worker, "initial_message"), "SECRET_PROMPT_BODY\n")

	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	setModTime(t, filepath.Join(worker, "status"), now.Add(-time.Minute))
	setModTime(t, filepath.Join(worker, "message"), now.Add(-30*time.Second))

	state := dashboard.Collector{
		FleetRoot:  root,
		Now:        func() time.Time { return now },
		StaleAfter: 15 * time.Minute,
	}.Collect(t.Context())

	if len(state.Workers) != 1 {
		t.Fatalf("workers = %d, want 1; warnings: %v", len(state.Workers), state.Warnings)
	}
	got := state.Workers[0]
	if got.Task != "task-123" || got.Project != "api" || got.Health != "active" {
		t.Fatalf("worker identity/health = %#v", got)
	}
	if !got.Message.Present || !got.Message.UpdatedAt.Equal(now.Add(-30*time.Second)) {
		t.Fatalf("message metadata = %#v", got.Message)
	}
	if got.Message.Summary != "" {
		t.Fatalf("message body was projected: %q", got.Message.Summary)
	}
	serialized := dashboard.MustJSON(state)
	if strings.Contains(serialized, "Approval required") || strings.Contains(serialized, "hunter2") || strings.Contains(serialized, "SECRET_PROMPT_BODY") {
		t.Fatalf("state exposed a sensitive body: %s", serialized)
	}
}

func TestCollectorClassifiesStaleOrphanedAndCorruptWorkers(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

	stale := filepath.Join(root, "old-task", "api")
	mustMkdirAll(t, stale)
	writeFile(t, filepath.Join(stale, "status"), "in_progress\n")
	writeFile(t, filepath.Join(stale, "worktree"), root+"\n")
	setModTime(t, filepath.Join(stale, "status"), now.Add(-time.Hour))

	orphan := filepath.Join(root, "lost-task", "web")
	mustMkdirAll(t, orphan)
	writeFile(t, filepath.Join(orphan, "status"), "needs_input\n")
	writeFile(t, filepath.Join(orphan, "worktree"), filepath.Join(root, "missing")+"\n")
	setModTime(t, filepath.Join(orphan, "status"), now.Add(-time.Minute))

	corrupt := filepath.Join(root, "broken-task", "db")
	mustMkdirAll(t, corrupt)
	writeFile(t, filepath.Join(corrupt, "status"), strings.Repeat("x", 5000))

	state := dashboard.Collector{FleetRoot: root, Now: func() time.Time { return now }, StaleAfter: 15 * time.Minute}.Collect(t.Context())
	byTask := make(map[string]dashboard.Worker)
	for _, worker := range state.Workers {
		byTask[worker.Task] = worker
	}
	if byTask["old-task"].Health != "stale" {
		t.Errorf("old worker health = %q, want stale", byTask["old-task"].Health)
	}
	if byTask["lost-task"].Health != "orphaned" {
		t.Errorf("missing worktree health = %q, want orphaned", byTask["lost-task"].Health)
	}
	if len(state.Warnings) == 0 {
		t.Error("corrupt worker produced no warning")
	}
}

func TestCollectorToleratesMissingFleetRoot(t *testing.T) {
	state := dashboard.Collector{FleetRoot: filepath.Join(t.TempDir(), "missing")}.Collect(t.Context())
	if state.Workers == nil || len(state.Warnings) != 1 {
		t.Fatalf("state = %#v, want empty workers and one warning", state)
	}
}

func mustMkdirAll(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func setModTime(t *testing.T, path string, updated time.Time) {
	t.Helper()
	if err := os.Chtimes(path, updated, updated); err != nil {
		t.Fatal(err)
	}
}
