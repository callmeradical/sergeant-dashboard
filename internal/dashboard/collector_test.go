package dashboard_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
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

func TestCollectorsShareProcessWideProbeLimit(t *testing.T) {
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
	ready := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	runner := func(ctx context.Context, _ string, _ string, _ ...string) ([]byte, error) {
		now := active.Add(1)
		defer active.Add(-1)
		for {
			old := maximum.Load()
			if now <= old || maximum.CompareAndSwap(old, now) {
				break
			}
		}
		if now == 16 {
			once.Do(func() { close(ready) })
		}
		select {
		case <-release:
			return []byte(`{}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	var collections sync.WaitGroup
	collections.Add(2)
	for range 2 {
		go func() {
			defer collections.Done()
			dashboard.Collector{FleetRoot: root, Run: runner, ProbeTimeout: time.Second}.Collect(t.Context())
		}()
	}
	select {
	case <-ready:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("collectors did not reach the process-wide probe limit")
	}
	time.Sleep(25 * time.Millisecond)
	close(release)
	collections.Wait()
	if got := maximum.Load(); got > 16 {
		t.Fatalf("maximum active probes across collectors = %d, want at most 16", got)
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
	writeFile(t, filepath.Join(worker, "repository"), "acme/api\n")

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
	const workerCount = 41
	for index := 0; index < workerCount; index++ {
		worker := filepath.Join(root, fmt.Sprintf("task-%02d", index), "api")
		worktree := filepath.Join(root, fmt.Sprintf("worktree-%02d", index))
		mustMkdirAll(t, worker)
		mustMkdirAll(t, worktree)
		writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
		writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	}

	var active atomic.Int32
	var startedProbes atomic.Int32
	runner := func(ctx context.Context, _ string, _ string, _ ...string) ([]byte, error) {
		startedProbes.Add(1)
		active.Add(1)
		defer active.Add(-1)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	probeTimeout := 100 * time.Millisecond
	started := time.Now()
	state := dashboard.Collector{FleetRoot: root, Run: runner, ProbeTimeout: probeTimeout}.Collect(t.Context())
	elapsed := time.Since(started)

	if len(state.Workers) != workerCount {
		t.Fatalf("workers = %d, want complete %d-worker projection", len(state.Workers), workerCount)
	}
	if got, want := startedProbes.Load(), int32(3*workerCount); got != want {
		t.Fatalf("started probes = %d, want complete enrichment with %d probes", got, want)
	}
	if elapsed >= 5*probeTimeout {
		t.Fatalf("degraded collection took %v, want less than %v", elapsed, 5*probeTimeout)
	}
	if got := active.Load(); got != 0 {
		t.Fatalf("active probes after collection = %d, want 0", got)
	}
}

func TestCollectorAllocatesProbeTimeoutOnlyAcrossEnrichableWorkers(t *testing.T) {
	root := t.TempDir()
	const invalidWorkerCount = 40
	for index := 0; index < invalidWorkerCount; index++ {
		worker := filepath.Join(root, fmt.Sprintf("invalid-%02d", index), "api")
		mustMkdirAll(t, worker)
		status := "orphaned"
		if index%2 == 0 {
			status = "invalid"
		}
		writeFile(t, filepath.Join(worker, "status"), status+"\n")
		writeFile(t, filepath.Join(worker, "worktree"), filepath.Join(root, fmt.Sprintf("missing-%02d", index))+"\n")
	}
	healthyWorker := filepath.Join(root, "healthy", "api")
	healthyWorktree := filepath.Join(root, "healthy-worktree")
	mustMkdirAll(t, healthyWorker)
	mustMkdirAll(t, healthyWorktree)
	writeFile(t, filepath.Join(healthyWorker, "status"), "in_progress\n")
	writeFile(t, filepath.Join(healthyWorker, "worktree"), healthyWorktree+"\n")

	var invalidProbes atomic.Int32
	runner := func(ctx context.Context, dir, name string, _ ...string) ([]byte, error) {
		if dir != healthyWorktree {
			invalidProbes.Add(1)
			return nil, fmt.Errorf("unexpected probe for invalid worktree %q", dir)
		}
		select {
		case <-time.After(160 * time.Millisecond):
			if name == "no-mistakes" {
				return []byte("review"), nil
			}
			return []byte(`{"url":"https://github.com/acme/api/pull/7","state":"OPEN"}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	state := dashboard.Collector{FleetRoot: root, Run: runner, ProbeTimeout: 200 * time.Millisecond}.Collect(t.Context())
	if len(state.Workers) != invalidWorkerCount+1 {
		t.Fatalf("workers = %d, want complete %d-worker projection", len(state.Workers), invalidWorkerCount+1)
	}
	var healthy dashboard.Worker
	for _, worker := range state.Workers {
		if worker.Task == "healthy" {
			healthy = worker
			break
		}
	}
	if healthy.PullRequest.URL == "" || !healthy.NoMistakes.Available {
		t.Fatalf("healthy worker metadata incomplete: pull request=%#v no-mistakes=%#v", healthy.PullRequest, healthy.NoMistakes)
	}
	if got := invalidProbes.Load(); got != 0 {
		t.Fatalf("probes for non-enrichable workers = %d, want 0", got)
	}
}

func TestCollectorBoundsGoroutinesIndependentlyOfFleetSize(t *testing.T) {
	root := t.TempDir()
	for index := 0; index < 512; index++ {
		worker := filepath.Join(root, fmt.Sprintf("task-%03d", index), "api")
		worktree := filepath.Join(root, fmt.Sprintf("worktree-%03d", index))
		mustMkdirAll(t, worker)
		mustMkdirAll(t, worktree)
		writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
		writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	}

	release := make(chan struct{})
	ready := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	var once sync.Once
	runner := func(ctx context.Context, _ string, _ string, _ ...string) ([]byte, error) {
		now := active.Add(1)
		for {
			old := maximum.Load()
			if now <= old || maximum.CompareAndSwap(old, now) {
				break
			}
		}
		if now == 16 {
			once.Do(func() { close(ready) })
		}
		defer active.Add(-1)
		select {
		case <-release:
			return []byte(`{}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	baseline := runtime.NumGoroutine()
	done := make(chan struct{})
	go func() {
		dashboard.Collector{FleetRoot: root, Run: runner, ProbeTimeout: time.Second}.Collect(t.Context())
		close(done)
	}()
	select {
	case <-ready:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("collector did not reach its active probe limit")
	}

	maximumGrowth := 0
	measurement := time.NewTimer(25 * time.Millisecond)
	ticker := time.NewTicker(time.Millisecond)
measure:
	for {
		select {
		case <-ticker.C:
			maximumGrowth = max(maximumGrowth, runtime.NumGoroutine()-baseline)
		case <-measurement.C:
			break measure
		}
	}
	ticker.Stop()
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("collector did not release blocked probes")
	}

	if maximumGrowth > 40 {
		t.Fatalf("goroutine growth = %d, want at most 40 independently of fleet size", maximumGrowth)
	}
	if got := maximum.Load(); got > 16 {
		t.Fatalf("maximum active probes = %d, want at most 16", got)
	}
	if got := active.Load(); got != 0 {
		t.Fatalf("active probes after collection = %d, want 0", got)
	}
}

func TestCollectorCancellationReleasesProbes(t *testing.T) {
	root := t.TempDir()
	for index := 0; index < 8; index++ {
		worker := filepath.Join(root, fmt.Sprintf("task-%02d", index), "api")
		worktree := filepath.Join(root, fmt.Sprintf("worktree-%02d", index))
		mustMkdirAll(t, worker)
		mustMkdirAll(t, worktree)
		writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
		writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	}

	ready := make(chan struct{})
	var active atomic.Int32
	var once sync.Once
	runner := func(ctx context.Context, _ string, _ string, _ ...string) ([]byte, error) {
		if active.Add(1) == 16 {
			once.Do(func() { close(ready) })
		}
		defer active.Add(-1)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan dashboard.State, 1)
	go func() {
		done <- dashboard.Collector{FleetRoot: root, Run: runner, ProbeTimeout: time.Second}.Collect(ctx)
	}()
	select {
	case <-ready:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("collector did not start all probes")
	}
	cancel()
	select {
	case state := <-done:
		if len(state.Workers) != 8 {
			t.Fatalf("workers = %d, want complete 8-worker projection", len(state.Workers))
		}
	case <-time.After(time.Second):
		t.Fatal("collector did not return after cancellation")
	}
	if got := active.Load(); got != 0 {
		t.Fatalf("active probes after cancellation = %d, want 0", got)
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
	writeFile(t, filepath.Join(worker, "repository"), "acme/api\n")
	writeFile(t, filepath.Join(worker, "td_task"), "td-123\n")
	writeFile(t, filepath.Join(worker, "response_id"), "opaque-secret-id\n")
	writeFile(t, filepath.Join(worktree, "graphify-out", "GRAPH_REPORT.md"), "# Collector graph\ntoken=graph-secret\n")

	runner := func(_ context.Context, dir, name string, args ...string) ([]byte, error) {
		if dir != worktree {
			t.Fatalf("probe dir = %q, want %q", dir, worktree)
		}
		switch name {
		case "gh":
			return []byte(`{"url":"https://github.com/acme/api/pull/7","state":"OPEN","statusCheckRollup":[{"name":"test","status":"COMPLETED","conclusion":"SUCCESS"},{"context":"legacy-ci","state":"PENDING"}],"comments":[{"author":{"login":"reviewer"},"body":"ship it; password=private","url":"https://github.com/acme/api/pull/7#issuecomment-1"}]}`), nil
		case "no-mistakes":
			return []byte("run 42 review running token=unrecognized-secret\n"), nil
		case "td":
			return []byte("td-123: Dashboard\nCOMMENT: rollout approved\nAuthorization: Bearer td-secret\n"), nil
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
	if got.PullRequest.Checks[1].Context != "legacy-ci" {
		t.Fatalf("legacy status context = %q, want legacy-ci", got.PullRequest.Checks[1].Context)
	}
	if got.Repository != "acme/api" {
		t.Fatalf("repository = %q, want configured identity", got.Repository)
	}
	if !got.NoMistakes.Available || got.NoMistakes.Phase != "review" {
		t.Fatalf("no-mistakes = %#v", got.NoMistakes)
	}
	if !strings.Contains(got.NoMistakes.Summary, "run 42 review running token=[REDACTED]") || !strings.Contains(got.TD.Summary, "COMMENT: rollout approved") || strings.Contains(got.TD.Summary, "td-secret") {
		t.Fatalf("td/no-mistakes details = %#v / %#v", got.TD, got.NoMistakes)
	}
	if len(got.PullRequest.Comments) != 1 || strings.Contains(got.PullRequest.Comments[0].Body, "private") {
		t.Fatalf("pull request comments = %#v", got.PullRequest.Comments)
	}
	if !got.Graphify.Present || !strings.Contains(got.Graphify.Summary, "# Collector graph") || strings.Contains(got.Graphify.Summary, "graph-secret") || !got.OCInject.ResponsePending {
		t.Fatalf("graphify/oc-inject = %#v / %#v", got.Graphify, got.OCInject)
	}
	serialized := mustJSON(t, state)
	if strings.Contains(serialized, "opaque-secret-id") || strings.Contains(serialized, "unrecognized-secret") {
		t.Fatal("opaque source data was exposed")
	}
}

func TestCollectorBoundsFleetTraversalBeforeReadingWorkerMetadata(t *testing.T) {
	root := t.TempDir()
	task := filepath.Join(root, "task")
	for index := range 1001 {
		worker := filepath.Join(task, fmt.Sprintf("project-%04d", index))
		mustMkdirAll(t, worker)
		status := "done\n"
		if index == 1000 {
			status = "not-a-status\n"
		}
		writeFile(t, filepath.Join(worker, "status"), status)
	}

	state := dashboard.Collector{FleetRoot: root}.Collect(t.Context())
	if len(state.Workers) != 1000 {
		t.Fatalf("workers = %d, want safety bound 1000", len(state.Workers))
	}
	if !slices.Contains(state.Warnings, "fleet collection truncated at safety limit") {
		t.Fatalf("warnings = %q, want truthful truncation warning", state.Warnings)
	}
	for _, warning := range state.Warnings {
		if strings.Contains(warning, "project-1000") {
			t.Fatalf("collector read worker metadata beyond safety bound: %q", warning)
		}
	}
}

func TestCollectorResolvesConfiguredProjectAndRepositoryNames(t *testing.T) {
	root := t.TempDir()
	configRoot := t.TempDir()
	task := filepath.Join(root, "task-123")
	worker := filepath.Join(task, "api")
	mustMkdirAll(t, worker)
	writeFile(t, filepath.Join(task, "brief.md"), "Project: operator-suite\nBrief:   Repair production API\n")
	writeFile(t, filepath.Join(worker, "status"), "done\n")
	writeFile(t, filepath.Join(configRoot, "operator-suite.yaml"), "name: operator-suite\nrepos:\n  - name: api\n    path: /srv/api\n")

	state := dashboard.Collector{FleetRoot: root, ConfigRoot: configRoot}.Collect(t.Context())
	if len(state.Workers) != 1 {
		t.Fatalf("workers = %d, want 1", len(state.Workers))
	}
	got := state.Workers[0]
	if got.Project != "operator-suite" || got.Repository != "api" || got.Title != "Repair production API" {
		t.Fatalf("configured identity = %#v", got)
	}
}

func TestCollectorDoesNotGuessUnregisteredIdentity(t *testing.T) {
	root := t.TempDir()
	task := filepath.Join(root, "task-123")
	worker := filepath.Join(task, "opaque-repo-alias")
	mustMkdirAll(t, worker)
	writeFile(t, filepath.Join(task, "brief.md"), "Project: missing-project\nBrief:   Repair production API\n")
	writeFile(t, filepath.Join(worker, "status"), "done\n")

	state := dashboard.Collector{FleetRoot: root, ConfigRoot: t.TempDir()}.Collect(t.Context())
	got := state.Workers[0]
	if got.Project != "unavailable" || got.Repository != "unavailable" {
		t.Fatalf("unregistered identity = %#v, want unavailable labels", got)
	}
}

func TestCollectorRejectsAmbiguousOrIncompleteConfiguredIdentity(t *testing.T) {
	for _, test := range []struct {
		name   string
		brief  string
		config string
	}{
		{name: "repository path missing", brief: "Project: operator-suite\nBrief: Repair API\n", config: "name: operator-suite\nrepos:\n  - name: api\n"},
		{name: "duplicate project metadata", brief: "Project: operator-suite\nProject: another-suite\nBrief: Repair API\n", config: "name: operator-suite\nrepos:\n  - name: api\n    path: /srv/api\n"},
		{name: "duplicate brief metadata", brief: "Project: operator-suite\nBrief: Repair API\nBrief: Ignore this duplicate\n", config: "name: operator-suite\nrepos:\n  - name: api\n    path: /srv/api\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			configRoot := t.TempDir()
			task := filepath.Join(root, "task-123")
			worker := filepath.Join(task, "api")
			mustMkdirAll(t, worker)
			writeFile(t, filepath.Join(task, "brief.md"), test.brief)
			writeFile(t, filepath.Join(worker, "status"), "done\n")
			writeFile(t, filepath.Join(configRoot, "operator-suite.yaml"), test.config)

			got := dashboard.Collector{FleetRoot: root, ConfigRoot: configRoot}.CollectSummary(t.Context()).Workers[0]
			if got.Project != "unavailable" || got.Repository != "unavailable" {
				t.Fatalf("configured identity = %#v, want unavailable", got)
			}
		})
	}
}

func TestCollectorSortsConfiguredRepositoriesDeterministically(t *testing.T) {
	root := t.TempDir()
	configRoot := t.TempDir()
	task := filepath.Join(root, "task-123")
	mustMkdirAll(t, task)
	writeFile(t, filepath.Join(task, "brief.md"), "Project: operator-suite\nBrief: Repair services\n")
	for _, repository := range []string{"worker", "api"} {
		mustMkdirAll(t, filepath.Join(task, repository))
		writeFile(t, filepath.Join(task, repository, "status"), "done\n")
	}
	writeFile(t, filepath.Join(configRoot, "operator-suite.yaml"), "name: operator-suite\nrepos:\n  - name: worker\n    path: /srv/worker\n  - name: api\n    path: /srv/api\n")

	workers := dashboard.Collector{FleetRoot: root, ConfigRoot: configRoot}.CollectSummary(t.Context()).Workers
	if got := []string{workers[0].Repository, workers[1].Repository}; !slices.Equal(got, []string{"api", "worker"}) {
		t.Fatalf("repository order = %v, want [api worker]", got)
	}
}

func TestCollectorResolvesSameRepositoryNameAcrossProjectsAndRereadsConfigChanges(t *testing.T) {
	root := t.TempDir()
	configRoot := t.TempDir()
	for _, project := range []string{"alpha-suite", "beta-suite"} {
		task := filepath.Join(root, project+"-task")
		worker := filepath.Join(task, "api")
		mustMkdirAll(t, worker)
		writeFile(t, filepath.Join(task, "brief.md"), "Project: "+project+"\nBrief: Repair API\n")
		writeFile(t, filepath.Join(worker, "status"), "done\n")
		writeFile(t, filepath.Join(configRoot, project+".yaml"), "name: "+project+"\nrepos:\n  - name: api\n    path: /srv/"+project+"/api\n")
	}
	collector := dashboard.Collector{FleetRoot: root, ConfigRoot: configRoot}
	workers := collector.CollectSummary(t.Context()).Workers
	if got := []string{workers[0].Project + "/" + workers[0].Repository, workers[1].Project + "/" + workers[1].Repository}; !slices.Equal(got, []string{"alpha-suite/api", "beta-suite/api"}) {
		t.Fatalf("configured identities = %v", got)
	}

	writeFile(t, filepath.Join(configRoot, "alpha-suite.yaml"), "name: alpha-suite\nrepos:\n  - name: worker\n    path: /srv/alpha-suite/worker\n")
	workers = collector.CollectSummary(t.Context()).Workers
	if workers[0].Project != "unavailable" || workers[0].Repository != "unavailable" {
		t.Fatalf("identity after config change = %#v, want unavailable", workers[0])
	}
}

func TestCollectorKeepsSummaryTitlesCompact(t *testing.T) {
	root := t.TempDir()
	configRoot := t.TempDir()
	task := filepath.Join(root, "task-123")
	worker := filepath.Join(task, "api")
	mustMkdirAll(t, worker)
	writeFile(t, filepath.Join(task, "brief.md"), "Project: operator-suite\nBrief: Repair production API. This paragraph contains operational instructions that belong only in detail and must not expand the card summary beyond its concise title boundary.\n")
	writeFile(t, filepath.Join(worker, "status"), "done\n")
	writeFile(t, filepath.Join(configRoot, "operator-suite.yaml"), "name: operator-suite\nrepos:\n  - name: api\n    path: /srv/api\n")

	state := dashboard.Collector{FleetRoot: root, ConfigRoot: configRoot}.CollectSummary(t.Context())
	if got, want := state.Workers[0].Title, "Repair production API"; got != want {
		t.Fatalf("summary title = %q, want %q", got, want)
	}
}

func TestCollectorRejectsDetailPathsOutsideFleetRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	mustMkdirAll(t, filepath.Join(outside, "repo"))
	writeFile(t, filepath.Join(outside, "repo", "status"), "done\n")

	collector := dashboard.Collector{FleetRoot: root}
	if worker, ok := collector.CollectDetail(t.Context(), "..", filepath.Base(outside)); ok {
		t.Fatalf("outside detail = %#v, want rejected traversal", worker)
	}

	task := filepath.Join(root, "task")
	mustMkdirAll(t, task)
	if err := os.Symlink(filepath.Join(outside, "repo"), filepath.Join(task, "repo")); err != nil {
		t.Fatal(err)
	}
	if worker, ok := collector.CollectDetail(t.Context(), "task", "repo"); ok {
		t.Fatalf("symlinked detail = %#v, want rejected worker", worker)
	}

	symlinkRoot := t.TempDir()
	mustMkdirAll(t, filepath.Join(outside, "task", "repo"))
	writeFile(t, filepath.Join(outside, "task", "repo", "status"), "done\n")
	if err := os.Symlink(filepath.Join(outside, "task"), filepath.Join(symlinkRoot, "task")); err != nil {
		t.Fatal(err)
	}
	if worker, ok := (dashboard.Collector{FleetRoot: symlinkRoot}).CollectDetail(t.Context(), "task", "repo"); ok {
		t.Fatalf("intermediate symlink detail = %#v, want rejected worker", worker)
	}
}

func TestCollectorRejectsSymlinkedConfiguredIdentitySources(t *testing.T) {
	root := t.TempDir()
	configRoot := t.TempDir()
	task := filepath.Join(root, "task")
	worker := filepath.Join(task, "api")
	mustMkdirAll(t, worker)
	writeFile(t, filepath.Join(worker, "status"), "done\n")
	privateBrief := filepath.Join(task, "initial_message")
	writeFile(t, privateBrief, "Project: operator-suite\nBrief: Private prompt title\n")
	if err := os.Symlink("initial_message", filepath.Join(task, "brief.md")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(configRoot, "operator-suite.yaml"), "name: operator-suite\nrepos:\n  - name: api\n    path: /srv/api\n")

	got := dashboard.Collector{FleetRoot: root, ConfigRoot: configRoot}.CollectSummary(t.Context()).Workers[0]
	if got.Project != "unavailable" || got.Repository != "unavailable" {
		t.Fatalf("symlinked configured identity = %#v, want unavailable", got)
	}
}

func TestCollectorDoesNotFollowSymlinkedWorkerMetadata(t *testing.T) {
	root := t.TempDir()
	workerDir := filepath.Join(root, "task", "repo")
	mustMkdirAll(t, workerDir)
	writeFile(t, filepath.Join(workerDir, "status"), "done\n")
	writeFile(t, filepath.Join(workerDir, "initial_message"), "password=private-prompt\n")
	if err := os.Symlink("initial_message", filepath.Join(workerDir, "message")); err != nil {
		t.Fatal(err)
	}

	worker, ok := (dashboard.Collector{FleetRoot: root}).CollectDetail(t.Context(), "task", "repo")
	if !ok {
		t.Fatal("legitimate worker detail was unavailable")
	}
	if worker.Message.Present || strings.Contains(worker.Message.Summary, "private-prompt") {
		t.Fatalf("symlinked message = %#v, want unread", worker.Message)
	}
}

func TestCollectorDoesNotFollowSymlinkedGraphifyDirectory(t *testing.T) {
	root := t.TempDir()
	workerDir := filepath.Join(root, "task", "repo")
	worktree := filepath.Join(root, "worktree")
	mustMkdirAll(t, workerDir)
	mustMkdirAll(t, worktree)
	writeFile(t, filepath.Join(workerDir, "status"), "done\n")
	writeFile(t, filepath.Join(workerDir, "worktree"), worktree+"\n")
	privateGraph := filepath.Join(root, "private-graph")
	mustMkdirAll(t, privateGraph)
	writeFile(t, filepath.Join(privateGraph, "GRAPH_REPORT.md"), "token=private-graph-secret\n")
	if err := os.Symlink(privateGraph, filepath.Join(worktree, "graphify-out")); err != nil {
		t.Fatal(err)
	}

	worker, ok := (dashboard.Collector{FleetRoot: root, Run: func(context.Context, string, string, ...string) ([]byte, error) {
		return nil, errors.New("unavailable")
	}}).CollectDetail(t.Context(), "task", "repo")
	if !ok {
		t.Fatal("legitimate worker detail was unavailable")
	}
	if worker.Graphify.Status == "available" || strings.Contains(worker.Graphify.Summary, "private-graph-secret") {
		t.Fatalf("symlinked graphify report = %#v, want unread", worker.Graphify)
	}
}

func TestCollectorRejectsSameRootIntermediateSymlink(t *testing.T) {
	root := t.TempDir()
	privateTask := filepath.Join(root, "private-task")
	worker := filepath.Join(privateTask, "repo")
	mustMkdirAll(t, worker)
	writeFile(t, filepath.Join(worker, "status"), "done\n")
	writeFile(t, filepath.Join(worker, "message"), "password=private-prompt\n")
	if err := os.Symlink("private-task", filepath.Join(root, "public-task")); err != nil {
		t.Fatal(err)
	}

	if got, ok := (dashboard.Collector{FleetRoot: root}).CollectDetail(t.Context(), "public-task", "repo"); ok {
		t.Fatalf("same-root intermediate symlink detail = %#v, want rejected worker", got)
	}
}

func TestCollectorReportsUnreadableDetailMetadataTruthfully(t *testing.T) {
	root := t.TempDir()
	workerDir := filepath.Join(root, "task", "repo")
	mustMkdirAll(t, workerDir)
	writeFile(t, filepath.Join(workerDir, "status"), "done\n")
	message := filepath.Join(workerDir, "message")
	writeFile(t, message, "operator note\n")
	if err := os.Chmod(message, 0); err != nil {
		t.Fatal(err)
	}

	worker, ok := (dashboard.Collector{FleetRoot: root}).CollectDetail(t.Context(), "task", "repo")
	if !ok {
		t.Fatal("legitimate worker detail was unavailable")
	}
	if !worker.Message.Present || worker.Message.Status != "unreadable" {
		t.Fatalf("unreadable message = %#v, want present unreadable metadata", worker.Message)
	}
}

func TestCollectorHonorsCancellationBeforeFleetTraversal(t *testing.T) {
	root := t.TempDir()
	worker := filepath.Join(root, "task", "project")
	mustMkdirAll(t, worker)
	writeFile(t, filepath.Join(worker, "status"), "done\n")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	state := dashboard.Collector{FleetRoot: root}.Collect(ctx)
	if len(state.Workers) != 0 || !slices.Contains(state.Warnings, "fleet collection canceled") {
		t.Fatalf("canceled collection = %#v, want no workers and cancellation warning", state)
	}
}

func TestCollectorReportsUnavailableCorruptAndOversizedSourcesTruthfully(t *testing.T) {
	root := t.TempDir()
	worker := filepath.Join(root, "task-123", "api")
	worktree := filepath.Join(root, "worktree")
	mustMkdirAll(t, worker)
	mustMkdirAll(t, filepath.Join(worktree, "graphify-out"))
	writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
	writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	writeFile(t, filepath.Join(worker, "td_task"), "td-123\n")
	writeFile(t, filepath.Join(worktree, "graphify-out", "GRAPH_REPORT.md"), strings.Repeat("x", (64<<10)+1))

	runner := func(_ context.Context, _ string, name string, _ ...string) ([]byte, error) {
		switch name {
		case "gh":
			return []byte(`{"url":`), nil
		case "no-mistakes":
			return []byte(strings.Repeat("x", (64<<10)+1)), nil
		case "td":
			return []byte(" \n"), nil
		default:
			return nil, fmt.Errorf("unexpected probe %s", name)
		}
	}

	got := dashboard.Collector{FleetRoot: root, Run: runner}.Collect(t.Context()).Workers[0]
	if got.PullRequest.Status != "corrupt" || got.NoMistakes.Status != "oversized" || got.TD.Status != "corrupt" || got.TD.Summary != "[content unavailable: corrupt]" || got.Graphify.Status != "oversized" {
		t.Fatalf("degraded statuses = PR %q, no-mistakes %q, td %q, Graphify %q", got.PullRequest.Status, got.NoMistakes.Status, got.TD.Status, got.Graphify.Status)
	}
}

func TestCollectorPreservesOCInjectMetadataForNonEnrichableWorkers(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		status     string
		worktree   string
		wantStatus string
		wantHealth string
	}{
		{name: "active", status: "in_progress", wantStatus: "in_progress", wantHealth: "orphaned"},
		{name: "terminal", status: "done", worktree: filepath.Join(root, "removed-terminal"), wantStatus: "done", wantHealth: "complete"},
		{name: "unknown", status: "invalid", worktree: filepath.Join(root, "removed-unknown"), wantStatus: "unknown", wantHealth: "unknown"},
		{name: "orphaned", status: "orphaned", worktree: filepath.Join(root, "removed-orphaned"), wantStatus: "orphaned", wantHealth: "orphaned"},
	}
	for _, test := range tests {
		worker := filepath.Join(root, test.name, "api")
		mustMkdirAll(t, worker)
		writeFile(t, filepath.Join(worker, "status"), test.status+"\n")
		if test.worktree != "" {
			writeFile(t, filepath.Join(worker, "worktree"), test.worktree+"\n")
		}
		writeFile(t, filepath.Join(worker, "response_id"), "private-response-id\n")
		writeFile(t, filepath.Join(worker, "response_ack"), "private-response-ack\n")
		setModTime(t, filepath.Join(worker, "response_id"), now.Add(-time.Minute))
		setModTime(t, filepath.Join(worker, "response_ack"), now)
	}

	var probes atomic.Int32
	state := dashboard.Collector{
		FleetRoot: root,
		Run: func(context.Context, string, string, ...string) ([]byte, error) {
			probes.Add(1)
			return nil, nil
		},
	}.Collect(t.Context())

	byTask := make(map[string]dashboard.Worker, len(state.Workers))
	for _, worker := range state.Workers {
		byTask[worker.Task] = worker
	}
	for _, test := range tests {
		got := byTask[test.name]
		if got.Status != test.wantStatus {
			t.Errorf("%s status = %q, want %q", test.name, got.Status, test.wantStatus)
		}
		if got.Health != test.wantHealth {
			t.Errorf("%s health = %q, want %q", test.name, got.Health, test.wantHealth)
		}
		if !got.OCInject.ResponsePending || !got.OCInject.ResponseAcked ||
			!got.OCInject.ResponsePendingAt.Equal(now.Add(-time.Minute)) ||
			!got.OCInject.ResponseAckedAt.Equal(now) || !got.OCInject.UpdatedAt.Equal(now) {
			t.Errorf("%s oc-inject metadata = %#v", test.name, got.OCInject)
		}
	}
	if got := probes.Load(); got != 0 {
		t.Fatalf("probes for non-enrichable workers = %d, want 0", got)
	}
	serialized := mustJSON(t, state)
	if strings.Contains(serialized, "private-response-id") || strings.Contains(serialized, "private-response-ack") {
		t.Fatal("oc-inject response body was exposed")
	}
}

func TestRedactMetadataIsDeterministicAndRemovesSecrets(t *testing.T) {
	input := `{"Authorization":"Bearer super-secret","auth":"Basic dXNlcjpwYXNz","detail":"password=hunter2 safe-tail","event":"inject","nested":{"count":2,"prompt":"do not expose"},"token":"ghp_abcdefghijklmnopqrstuvwxyz0123456789"}`
	want := `{"Authorization":"[REDACTED]","auth":"[REDACTED]","detail":"password=[REDACTED]","event":"inject","nested":{"count":2,"prompt":"[REDACTED]"},"token":"[REDACTED]"}`
	first := dashboard.RedactText(input)
	second := dashboard.RedactText(input)
	if first != want || second != want {
		t.Fatalf("redacted metadata = %s / %s, want %s", first, second, want)
	}
}

func TestRedactMetadataHandlesAdversarialOperationalText(t *testing.T) {
	input := strings.Join([]string{
		"ordinary deployment prose remains useful",
		`API_TOKEN="line one`,
		`line two"`,
		"remote=https://operator:password@example.com/repository",
		"token-remote=https://opaque-token@example.com/repository",
		"database=postgres://operator:password@example.com/repository",
		`note password="correct horse battery staple"`,
		"GITHUB_TOKEN=github_pat_abcdefghijklmnopqrstuvwxyz0123456789",
		"Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature",
		"authorization=Basic dXNlcjpwYXNzd29yZA==",
		`request Authorization: Digest response="digest-secret"`,
		"inline Basic dXNlcjpwYXNzd29yZA== credential",
		"-----BEGIN PRIVATE KEY-----",
		"private-key-material",
		"-----END PRIVATE KEY-----",
		`password="quoted value with spaces"`,
		"AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE",
		"SLACK_TOKEN=xoxb-123456789012345678901234",
		"control:\x00byte",
	}, "\n")
	redacted := dashboard.RedactText(input)
	for _, secret := range []string{"line one", "line two", "operator:password", "opaque-token", "correct horse", "dXNlcjpwYXNzd29yZA", "digest-secret", "private-key-material", "github_pat_", "eyJhbGci", "quoted value", "AKIAIOSFODNN7EXAMPLE", "xoxb-", "\x00"} {
		if strings.Contains(redacted, secret) {
			t.Errorf("redacted text retained %q: %q", secret, redacted)
		}
	}
	for _, useful := range []string{"ordinary deployment prose remains useful", "remote=https://[REDACTED]@example.com/repository", "API_TOKEN=[REDACTED]", "control:byte"} {
		if !strings.Contains(redacted, useful) {
			t.Errorf("redacted text omitted %q: %q", useful, redacted)
		}
	}
	if redacted != dashboard.RedactText(input) {
		t.Fatal("redaction is not deterministic")
	}
	jsonEnvironment := `{"PATH":"/usr/bin","API_TOKEN":"opaque-json-secret","DATABASE_URL":"postgres://user:password@db/internal"}`
	redactedEnvironment := dashboard.RedactText(jsonEnvironment)
	if strings.Contains(redactedEnvironment, "opaque-json-secret") || strings.Contains(redactedEnvironment, "user:password") || !strings.Contains(redactedEnvironment, `"PATH":"/usr/bin"`) {
		t.Fatalf("redacted JSON environment = %s", redactedEnvironment)
	}
	nestedEnvironment := dashboard.RedactText(`{"environment":{"PATH":"/usr/bin","TOKEN":"nested-secret"}}`)
	if strings.Contains(nestedEnvironment, "nested-secret") || !strings.Contains(nestedEnvironment, `"PATH":"/usr/bin"`) {
		t.Fatalf("redacted nested environment = %s", nestedEnvironment)
	}
	ordinaryFields := dashboard.RedactText(`{"compass":"north","message":"deployment complete","body":"ordinary prose"}`)
	for _, useful := range []string{"north", "deployment complete", "ordinary prose"} {
		if !strings.Contains(ordinaryFields, useful) {
			t.Fatalf("redaction erased ordinary field %q: %s", useful, ordinaryFields)
		}
	}
	camelCaseSecrets := dashboard.RedactText(`{"accessToken":"opaque-access","clientSecret":"opaque-client","apiKey":"opaque-api","refreshToken":"opaque-refresh","githubToken":"opaque-github","dbPassword":"opaque-db"}`)
	for _, secret := range []string{"opaque-access", "opaque-client", "opaque-api", "opaque-refresh", "opaque-github", "opaque-db"} {
		if strings.Contains(camelCaseSecrets, secret) {
			t.Fatalf("camelCase secret %q was exposed: %s", secret, camelCaseSecrets)
		}
	}
}

func TestCollectorProjectsBoundedOperationalFilesWithoutReadingPromptOrResponseBodies(t *testing.T) {
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
	writeFile(t, filepath.Join(worker, "diagnostic"), "collection delayed; retry safe\n")
	writeFile(t, filepath.Join(worker, "worker.log"), "tests passed; token=private-token\n")
	writeFile(t, filepath.Join(worker, "handoff"), "remaining: open PR\n")
	writeFile(t, filepath.Join(worker, "initial_message"), "SECRET_PROMPT_BODY\n")
	writeFile(t, filepath.Join(worker, "response"), "SECRET_RESPONSE_BODY\n")
	writeFile(t, filepath.Join(worktree, ".env"), "REPOSITORY_SECRET=do-not-read\n")

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
	if got.Message.Summary != "Approval required; password=[REDACTED]" || got.Diagnostic.Summary != "collection delayed; retry safe" ||
		got.Log.Summary != "tests passed; token=[REDACTED]" || got.Handoff.Summary != "remaining: open PR" {
		t.Fatalf("operational files = %#v / %#v / %#v / %#v", got.Message, got.Diagnostic, got.Log, got.Handoff)
	}
	serialized := mustJSON(t, state)
	if strings.Contains(serialized, "hunter2") || strings.Contains(serialized, "SECRET_PROMPT_BODY") || strings.Contains(serialized, "SECRET_RESPONSE_BODY") || strings.Contains(serialized, "do-not-read") {
		t.Fatalf("state exposed a sensitive body: %s", serialized)
	}
}

func TestCollectorReportsExistingInvalidOperationalFile(t *testing.T) {
	root := t.TempDir()
	worker := filepath.Join(root, "task-123", "api")
	mustMkdirAll(t, worker)
	writeFile(t, filepath.Join(worker, "status"), "orphaned\n")
	mustMkdirAll(t, filepath.Join(worker, "message"))
	writeFile(t, filepath.Join(worker, "diagnostic"), "")

	workerState := dashboard.Collector{FleetRoot: root}.Collect(t.Context()).Workers[0]
	message := workerState.Message
	if !message.Present || message.Status != "corrupt" {
		t.Fatalf("invalid operational file = %#v, want present corrupt status", message)
	}
	if !workerState.Diagnostic.Present || workerState.Diagnostic.Status != "corrupt" {
		t.Fatalf("empty operational file = %#v, want present corrupt status", workerState.Diagnostic)
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

func TestCollectorClassifiesSummaryHealthFromLiveSupervisorEvidence(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		task       string
		status     string
		pane       string
		logAge     time.Duration
		statusAge  time.Duration
		worktree   bool
		wantHealth string
	}{
		{task: "exact-live", status: "in_progress", pane: "live", statusAge: time.Hour, worktree: true, wantHealth: "active"},
		{task: "dead-recent", status: "in_progress", pane: "dead", logAge: time.Minute, statusAge: time.Hour, worktree: true, wantHealth: "active"},
		{task: "collision-old", status: "in_progress", pane: "collision", logAge: time.Hour, statusAge: time.Hour, worktree: true, wantHealth: "stale"},
		{task: "waiting-live", status: "blocked", pane: "live", statusAge: time.Hour, worktree: true, wantHealth: "active"},
		{task: "terminal-live", status: "done", pane: "live", statusAge: time.Hour, worktree: true, wantHealth: "complete"},
		{task: "unsupported", status: "in_progress", pane: "unsupported", logAge: time.Minute, statusAge: time.Hour, worktree: true, wantHealth: "unknown"},
		{task: "missing-worktree", status: "in_progress", pane: "dead", logAge: time.Hour, statusAge: time.Hour, wantHealth: "orphaned"},
	}
	for _, test := range tests {
		worker := filepath.Join(root, test.task, "api")
		mustMkdirAll(t, worker)
		writeFile(t, filepath.Join(worker, "status"), test.status+"\n")
		writeFile(t, filepath.Join(worker, "pane"), test.pane+"\n")
		setModTime(t, filepath.Join(worker, "status"), now.Add(-test.statusAge))
		if test.logAge > 0 {
			writeFile(t, filepath.Join(worker, "worker.log"), "privacy-safe activity\n")
			setModTime(t, filepath.Join(worker, "worker.log"), now.Add(-test.logAge))
		}
		if test.worktree {
			worktree := filepath.Join(root, test.task+"-worktree")
			mustMkdirAll(t, worktree)
			writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
		}
	}

	inspected := make(map[string]int)
	collector := dashboard.Collector{
		FleetRoot:  root,
		Now:        func() time.Time { return now },
		StaleAfter: 15 * time.Minute,
		InspectSupervisor: func(_ context.Context, pane, stateDir string) (bool, error) {
			inspected[filepath.Base(filepath.Dir(stateDir))]++
			switch pane {
			case "live":
				return true, nil
			case "unsupported":
				return false, errors.New("process inspection unavailable")
			default:
				return false, nil
			}
		},
	}
	state := collector.CollectSummary(t.Context())
	byTask := make(map[string]dashboard.Worker, len(state.Workers))
	for _, worker := range state.Workers {
		byTask[worker.Task] = worker
	}
	for _, test := range tests {
		if got := byTask[test.task].Health; got != test.wantHealth {
			t.Errorf("%s health = %q, want %q", test.task, got, test.wantHealth)
		}
	}
	if inspected["terminal-live"] != 0 {
		t.Fatal("terminal worker should not require live process inspection")
	}
}

func TestCollectorDetailUsesSameLiveHealthAsSummary(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	workerDir := filepath.Join(root, "task", "api")
	worktree := filepath.Join(root, "worktree")
	mustMkdirAll(t, workerDir)
	mustMkdirAll(t, worktree)
	writeFile(t, filepath.Join(workerDir, "status"), "in_progress\n")
	writeFile(t, filepath.Join(workerDir, "worktree"), worktree+"\n")
	writeFile(t, filepath.Join(workerDir, "pane"), "live\n")
	setModTime(t, filepath.Join(workerDir, "status"), now.Add(-time.Hour))
	collector := dashboard.Collector{
		FleetRoot:         root,
		Now:               func() time.Time { return now },
		StaleAfter:        15 * time.Minute,
		InspectSupervisor: func(context.Context, string, string) (bool, error) { return true, nil },
	}

	summary := collector.CollectSummary(t.Context()).Workers[0]
	detail, ok := collector.CollectDetail(t.Context(), "task", "api")
	if !ok || detail.Health != summary.Health || detail.Health != "active" {
		t.Fatalf("summary/detail health = %q/%q ok=%t, want active/active", summary.Health, detail.Health, ok)
	}
}

func TestCollectorMatchesSupervisorCommandToExactFleetState(t *testing.T) {
	stateDir := "/tmp/fleet/task/repository"
	for _, test := range []struct {
		name    string
		command string
		want    bool
	}{
		{name: "exact", command: "/opt/sergeant/bin/sgt-worker /tmp/fleet/task/repository /tmp/worktree opencode message", want: true},
		{name: "quoted exact", command: `'/opt/sergeant/bin/sgt-worker' '/tmp/fleet/task/repository' /tmp/worktree`, want: true},
		{name: "colliding state", command: "/opt/sergeant/bin/sgt-worker /tmp/fleet/task/repository-old /tmp/worktree"},
		{name: "colliding process", command: "/opt/sergeant/bin/sgt-worker-old /tmp/fleet/task/repository /tmp/worktree"},
		{name: "unrelated command arguments", command: "echo /opt/sergeant/bin/sgt-worker /tmp/fleet/task/repository"},
		{name: "dead", command: "1|/opt/sergeant/bin/sgt-worker /tmp/fleet/task/repository /tmp/worktree"},
	} {
		t.Run(test.name, func(t *testing.T) {
			output := test.command
			if !strings.Contains(output, "|") {
				output = "0|" + output
			}
			live, err := dashboard.ParseSupervisorEvidence(output, stateDir)
			if err != nil || live != test.want {
				t.Fatalf("supervisor evidence = %t, %v; want %t", live, err, test.want)
			}
		})
	}
}

func TestCollectorReportsSummaryProbeFailures(t *testing.T) {
	root := t.TempDir()
	worker := filepath.Join(root, "task", "api")
	worktree := filepath.Join(root, "worktree")
	mustMkdirAll(t, worker)
	mustMkdirAll(t, worktree)
	writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
	writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	writeFile(t, filepath.Join(worker, "pane"), "pane\n")
	state := dashboard.Collector{
		FleetRoot: root,
		Run: func(context.Context, string, string, ...string) ([]byte, error) {
			return nil, errors.New("probe unavailable")
		},
		InspectSupervisor: func(context.Context, string, string) (bool, error) { return true, nil },
	}.CollectSummary(t.Context())
	if !slices.Contains(state.Warnings, "worker task/api summary probe failed") {
		t.Fatalf("warnings = %q, want summary probe failure", state.Warnings)
	}
}

func TestCollectorClassifiesTerminalWorkersCompleteAfterWorktreeCleanup(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name       string
		status     string
		wantHealth string
	}{
		{name: "done", status: "done", wantHealth: "complete"},
		{name: "failed", status: "failed", wantHealth: "complete"},
		{name: "failed-with-reason", status: "failed: command exited", wantHealth: "complete"},
		{name: "in-progress", status: "in_progress", wantHealth: "orphaned"},
	}
	for _, test := range tests {
		worker := filepath.Join(root, test.name, "api")
		mustMkdirAll(t, worker)
		writeFile(t, filepath.Join(worker, "status"), test.status+"\n")
		writeFile(t, filepath.Join(worker, "worktree"), filepath.Join(root, "removed-"+test.name)+"\n")
	}

	var probes atomic.Int32
	state := dashboard.Collector{
		FleetRoot: root,
		Run: func(context.Context, string, string, ...string) ([]byte, error) {
			probes.Add(1)
			return nil, nil
		},
	}.Collect(t.Context())

	byTask := make(map[string]dashboard.Worker, len(state.Workers))
	for _, worker := range state.Workers {
		byTask[worker.Task] = worker
	}
	for _, test := range tests {
		if got := byTask[test.name].Health; got != test.wantHealth {
			t.Errorf("%s health = %q, want %q", test.name, got, test.wantHealth)
		}
	}
	if got := probes.Load(); got != 0 {
		t.Fatalf("probes for missing worktrees = %d, want 0", got)
	}
}

func TestCollectorClassifiesLifecycleValues(t *testing.T) {
	root := t.TempDir()
	worktree := filepath.Join(root, "worktree")
	mustMkdirAll(t, worktree)
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		value      string
		wantStatus string
		wantHealth string
	}{
		{name: "in-progress", value: "in_progress", wantStatus: "in_progress", wantHealth: "active"},
		{name: "needs-input", value: "needs_input", wantStatus: "needs_input", wantHealth: "active"},
		{name: "blocked", value: "blocked", wantStatus: "blocked", wantHealth: "active"},
		{name: "done", value: "done", wantStatus: "done", wantHealth: "complete"},
		{name: "failed", value: "failed", wantStatus: "failed", wantHealth: "complete"},
		{name: "failed-with-reason", value: "failed: command exited", wantStatus: "failed: command exited", wantHealth: "complete"},
		{name: "orphaned", value: "orphaned", wantStatus: "orphaned", wantHealth: "orphaned"},
		{name: "empty", value: "", wantStatus: "unknown", wantHealth: "unknown"},
		{name: "unknown", value: "credential=hunter2", wantStatus: "unknown", wantHealth: "unknown"},
		{name: "oversized", value: strings.Repeat("x", 5000), wantStatus: "unknown", wantHealth: "unknown"},
	}

	for _, test := range tests {
		worker := filepath.Join(root, test.name, "api")
		mustMkdirAll(t, worker)
		writeFile(t, filepath.Join(worker, "status"), test.value+"\n")
		if test.wantHealth == "active" {
			writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
		}
		setModTime(t, filepath.Join(worker, "status"), now.Add(-time.Minute))
	}

	state := dashboard.Collector{
		FleetRoot:  root,
		Now:        func() time.Time { return now },
		StaleAfter: 15 * time.Minute,
		Run:        func(context.Context, string, string, ...string) ([]byte, error) { return nil, nil },
	}.Collect(t.Context())
	byTask := make(map[string]dashboard.Worker, len(state.Workers))
	for _, worker := range state.Workers {
		byTask[worker.Task] = worker
	}
	for _, test := range tests {
		worker := byTask[test.name]
		if worker.Status != test.wantStatus || worker.Health != test.wantHealth {
			t.Errorf("%s lifecycle = status %q health %q, want %q/%q", test.name, worker.Status, worker.Health, test.wantStatus, test.wantHealth)
		}
	}
	if serialized := mustJSON(t, state.Warnings); strings.Contains(serialized, "credential=hunter2") || strings.Contains(serialized, strings.Repeat("x", 100)) {
		t.Fatalf("warnings exposed raw corrupt lifecycle values: %s", serialized)
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
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
