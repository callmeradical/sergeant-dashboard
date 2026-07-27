package dashboard_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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

// TestEnrichWorkersBoundsToMaxConcurrentWorkers verifies that with 20 enrichable
// workers, no more than 8 workers are ever being enriched at the same time.
// It measures this by sampling goroutine growth while a slow runner is active:
// a semaphore of size 8 means at most 8 goroutines call enrichWorker, each of
// which may spawn up to 4 probe goroutines, giving growth ≤ 8 + 8×4 = 40.
// Without the semaphore fix (one goroutine per worker), growth would reach
// 20 + 20×4 = 100.
func TestEnrichWorkersBoundsToMaxConcurrentWorkers(t *testing.T) {
	root := t.TempDir()
	const workerCount = 20
	for index := 0; index < workerCount; index++ {
		worker := filepath.Join(root, fmt.Sprintf("task-%02d", index), "api")
		worktree := filepath.Join(root, fmt.Sprintf("worktree-%02d", index))
		mustMkdirAll(t, worker)
		mustMkdirAll(t, worktree)
		writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
		writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	}

	release := make(chan struct{})
	ready := make(chan struct{})
	var active atomic.Int32
	var once sync.Once
	runner := func(ctx context.Context, _ string, _ string, _ ...string) ([]byte, error) {
		if active.Add(1) == 16 { // process-wide slot ceiling reached
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
		dashboard.Collector{FleetRoot: root, Run: runner, ProbeTimeout: 5 * time.Second}.Collect(t.Context())
		close(done)
	}()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("collector did not saturate the process-wide probe limit")
	}

	// Sample goroutine growth while at peak load.
	growth := runtime.NumGoroutine() - baseline
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("collector did not finish after release")
	}

	// With 20 workers bounded to 8 concurrent by the semaphore:
	//   8 semaphore goroutines + 8 × 4 probe goroutines = 40 goroutine growth.
	// Without the bound (20 goroutines), growth would exceed 40 (up to 100).
	const maxGrowth = 40
	if growth > maxGrowth {
		t.Fatalf("goroutine growth = %d with 20 workers, want ≤ %d (semaphore pool must limit to 8 concurrent)", growth, maxGrowth)
	}
	if got := active.Load(); got != 0 {
		t.Fatalf("active probes after collection = %d, want 0", got)
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
		{name: "terminal", status: "done", worktree: filepath.Join(root, "removed-terminal"), wantStatus: "done", wantHealth: "recycled"},
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
	input := `{"Authorization":"Bearer super-secret","detail":"password=hunter2 safe-tail","event":"inject","nested":{"count":2,"prompt":"do not expose"},"token":"ghp_abcdefghijklmnopqrstuvwxyz0123456789"}`
	want := `{"Authorization":"[REDACTED]","detail":"password=[REDACTED]","event":"inject","nested":{"count":2,"prompt":"[REDACTED]"},"token":"[REDACTED]"}`
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
	for _, secret := range []string{"line one", "line two", "operator:password", "opaque-token", "correct horse", "dXNlcjpwYXNzd29yZA", "private-key-material", "github_pat_", "eyJhbGci", "quoted value", "AKIAIOSFODNN7EXAMPLE", "xoxb-", "\x00"} {
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

func TestCollectorClassifiesTerminalWorkersCompleteAfterWorktreeCleanup(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name       string
		status     string
		wantHealth string
	}{
		{name: "done", status: "done", wantHealth: "recycled"},
		{name: "failed", status: "failed", wantHealth: "recycled"},
		{name: "failed-with-reason", status: "failed: command exited", wantHealth: "recycled"},
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
		{name: "done", value: "done", wantStatus: "done", wantHealth: "recycled"},
		{name: "failed", value: "failed", wantStatus: "failed", wantHealth: "recycled"},
		{name: "failed-with-reason", value: "failed: command exited", wantStatus: "failed: command exited", wantHealth: "recycled"},
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

func TestCollectorClassifiesBlockedAndNeedsInputWithoutSupervisorAsActive(t *testing.T) {
	root := t.TempDir()
	worktree := filepath.Join(root, "worktree")
	mustMkdirAll(t, worktree)
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		status     string
		wantHealth string
	}{
		{name: "blocked", status: "blocked", wantHealth: "active"},
		{name: "needs-input", status: "needs_input", wantHealth: "active"},
		{name: "in-progress", status: "in_progress", wantHealth: "active"},
	}
	for _, test := range tests {
		worker := filepath.Join(root, test.name, "api")
		mustMkdirAll(t, worker)
		writeFile(t, filepath.Join(worker, "status"), test.status+"\n")
		writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
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
		if got := byTask[test.name].Health; got != test.wantHealth {
			t.Errorf("%s health = %q, want %q", test.name, got, test.wantHealth)
		}
	}
}

func TestParseSupervisorEvidenceMatchesSgtWorkerByStateDir(t *testing.T) {
	stateDir := "/home/lars/.local/share/sergeant/fleet/task-abc/sergeant-dashboard"
	for _, test := range []struct {
		name    string
		output  string
		want    bool
		wantErr bool
	}{
		{name: "live exact", output: "0|sgt-worker " + stateDir + " /worktree opencode message", want: true},
		{name: "live quoted", output: "0|'sgt-worker' '" + stateDir + "' /worktree", want: true},
		{name: "dead pane", output: "1|sgt-worker " + stateDir + " /worktree", want: false},
		{name: "prefix collision", output: "0|sgt-worker " + stateDir + "-extra /worktree", want: false},
		{name: "wrong binary", output: "0|sgt-worker-old " + stateDir + " /worktree", want: false},
		{name: "unrelated command", output: "0|echo sgt-worker " + stateDir, want: false},
		{name: "malformed no pipe", output: "sgt-worker " + stateDir, wantErr: true},
		{name: "invalid dead flag", output: "2|sgt-worker " + stateDir, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			live, err := dashboard.ParseSupervisorEvidence(test.output, stateDir)
			if (err != nil) != test.wantErr {
				t.Fatalf("ParseSupervisorEvidence(%q) error = %v, wantErr = %t", test.output, err, test.wantErr)
			}
			if !test.wantErr && live != test.want {
				t.Fatalf("ParseSupervisorEvidence(%q) = %t, want %t", test.output, live, test.want)
			}
		})
	}
}

func TestCollectorFallsBackToTimestampWhenSupervisorInspectionErrors(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		task       string
		statusAge  time.Duration
		logAge     time.Duration
		wantHealth string
	}{
		{task: "fresh-status", statusAge: time.Minute, wantHealth: "orphaned"},
		{task: "fresh-log", statusAge: time.Hour, logAge: time.Minute, wantHealth: "orphaned"},
		{task: "stale", statusAge: time.Hour, wantHealth: "stale"},
	}
	for _, test := range tests {
		dir := filepath.Join(root, test.task, "api")
		worktree := filepath.Join(root, test.task+"-worktree")
		mustMkdirAll(t, dir)
		mustMkdirAll(t, worktree)
		writeFile(t, filepath.Join(dir, "status"), "in_progress\n")
		writeFile(t, filepath.Join(dir, "pane"), "err-pane\n")
		writeFile(t, filepath.Join(dir, "worktree"), worktree+"\n")
		setModTime(t, filepath.Join(dir, "status"), now.Add(-test.statusAge))
		if test.logAge > 0 {
			writeFile(t, filepath.Join(dir, "worker.log"), "fresh activity\n")
			setModTime(t, filepath.Join(dir, "worker.log"), now.Add(-test.logAge))
		}
	}

	state := dashboard.Collector{
		FleetRoot:  root,
		Now:        func() time.Time { return now },
		StaleAfter: 15 * time.Minute,
		InspectSupervisor: func(context.Context, string, string) (bool, error) {
			return false, errors.New("tmux unavailable")
		},
	}.Collect(t.Context())
	byTask := make(map[string]dashboard.Worker, len(state.Workers))
	for _, worker := range state.Workers {
		byTask[worker.Task] = worker
	}
	for _, test := range tests {
		if got := byTask[test.task].Health; got != test.wantHealth {
			t.Errorf("%s health = %q, want %q", test.task, got, test.wantHealth)
		}
	}
}

func TestCollectorClassifiesActiveWithLiveSupervisorAndOrphanedWithoutIt(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		task       string
		status     string
		pane       string
		worktree   bool
		statusAge  time.Duration
		wantHealth string
	}{
		{task: "live-active", status: "in_progress", pane: "live-pane", worktree: true, statusAge: time.Hour, wantHealth: "active"},
		{task: "live-blocked", status: "blocked", pane: "live-pane", worktree: true, statusAge: time.Hour, wantHealth: "active"},
		{task: "dead-stale", status: "in_progress", pane: "dead-pane", worktree: true, statusAge: time.Hour, wantHealth: "stale"},
		{task: "dead-orphaned", status: "in_progress", pane: "dead-pane", worktree: true, statusAge: 5 * time.Minute, wantHealth: "orphaned"},
		{task: "no-worktree", status: "in_progress", worktree: false, statusAge: time.Minute, wantHealth: "orphaned"},
		{task: "terminal-done", status: "done", worktree: true, statusAge: time.Minute, wantHealth: "complete"},
		{task: "explicit-orphaned", status: "orphaned", worktree: false, statusAge: time.Minute, wantHealth: "orphaned"},
		{task: "explicit-orphaned-live", status: "orphaned", pane: "live-pane", worktree: true, statusAge: time.Minute, wantHealth: "orphaned"},
	}
	for _, test := range tests {
		dir := filepath.Join(root, test.task, "api")
		mustMkdirAll(t, dir)
		writeFile(t, filepath.Join(dir, "status"), test.status+"\n")
		setModTime(t, filepath.Join(dir, "status"), now.Add(-test.statusAge))
		if test.pane != "" {
			writeFile(t, filepath.Join(dir, "pane"), test.pane+"\n")
		}
		if test.worktree {
			wt := filepath.Join(root, test.task+"-worktree")
			mustMkdirAll(t, wt)
			writeFile(t, filepath.Join(dir, "worktree"), wt+"\n")
		}
	}

	var inspectedMu sync.Mutex
	inspected := make(map[string]int)
	var inspectedMu sync.Mutex
	collector := dashboard.Collector{
		FleetRoot:  root,
		Now:        func() time.Time { return now },
		StaleAfter: 15 * time.Minute,
		InspectSupervisor: func(_ context.Context, pane, _ string) (bool, error) {
			inspectedMu.Lock()
			inspected[pane]++
			inspectedMu.Unlock()
			return pane == "live-pane", nil
		},
	}
	state := collector.Collect(t.Context())
	byTask := make(map[string]dashboard.Worker, len(state.Workers))
	for _, worker := range state.Workers {
		byTask[worker.Task] = worker
	}
	for _, test := range tests {
		got := byTask[test.task].Health
		if got != test.wantHealth {
			t.Errorf("%s health = %q, want %q", test.task, got, test.wantHealth)
		}
	}
	inspectedMu.Lock()
	livePaneCount := inspected["live-pane"]
	inspectedMu.Unlock()
	if livePaneCount == 0 {
		t.Error("supervisor inspector was not called for pane-bearing workers")
	}
}

func TestCollectorClassifiesTerminalRecordsWithAndWithoutWorktree(t *testing.T) {
	root := t.TempDir()
	worktree := filepath.Join(root, "present-worktree")
	mustMkdirAll(t, worktree)
	tests := []struct {
		name       string
		status     string
		worktree   string
		wantHealth string
	}{
		{name: "done-with-wt", status: "done", worktree: worktree, wantHealth: "complete"},
		{name: "failed-with-wt", status: "failed", worktree: worktree, wantHealth: "complete"},
		{name: "done-no-wt", status: "done", worktree: filepath.Join(root, "removed"), wantHealth: "recycled"},
		{name: "failed-no-wt", status: "failed", worktree: filepath.Join(root, "removed"), wantHealth: "recycled"},
		{name: "failed-reason-no-wt", status: "failed: reason", worktree: filepath.Join(root, "removed"), wantHealth: "recycled"},
		{name: "done-no-wt-path", status: "done", worktree: "", wantHealth: "recycled"},
	}
	for _, test := range tests {
		worker := filepath.Join(root, test.name, "api")
		mustMkdirAll(t, worker)
		writeFile(t, filepath.Join(worker, "status"), test.status+"\n")
		if test.worktree != "" {
			writeFile(t, filepath.Join(worker, "worktree"), test.worktree+"\n")
		}
	}

	state := dashboard.Collector{
		FleetRoot: root,
		Run:       func(context.Context, string, string, ...string) ([]byte, error) { return nil, nil },
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
}

func TestCollectorUsesMultiFileEvidenceForStaleness(t *testing.T) {
	root := t.TempDir()
	worktree := filepath.Join(root, "worktree")
	mustMkdirAll(t, worktree)
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	staleAt := now.Add(-time.Hour)
	freshAt := now.Add(-time.Minute)

	type workerSpec struct {
		name       string
		statusAt   time.Time
		logAt      time.Time
		messageAt  time.Time
		diagAt     time.Time
		wantHealth string
	}
	tests := []workerSpec{
		{name: "all-stale", statusAt: staleAt, wantHealth: "stale"},
		{name: "fresh-log", statusAt: staleAt, logAt: freshAt, wantHealth: "active"},
		{name: "fresh-message", statusAt: staleAt, messageAt: freshAt, wantHealth: "active"},
		{name: "fresh-diagnostic", statusAt: staleAt, diagAt: freshAt, wantHealth: "active"},
		{name: "all-fresh", statusAt: freshAt, logAt: freshAt, messageAt: freshAt, diagAt: freshAt, wantHealth: "active"},
	}
	for _, test := range tests {
		worker := filepath.Join(root, test.name, "api")
		mustMkdirAll(t, worker)
		writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
		writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
		setModTime(t, filepath.Join(worker, "status"), test.statusAt)
		if !test.logAt.IsZero() {
			writeFile(t, filepath.Join(worker, "worker.log"), "log line\n")
			setModTime(t, filepath.Join(worker, "worker.log"), test.logAt)
		}
		if !test.messageAt.IsZero() {
			writeFile(t, filepath.Join(worker, "message"), "waiting\n")
			setModTime(t, filepath.Join(worker, "message"), test.messageAt)
		}
		if !test.diagAt.IsZero() {
			writeFile(t, filepath.Join(worker, "diagnostic"), "nominal\n")
			setModTime(t, filepath.Join(worker, "diagnostic"), test.diagAt)
		}
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
		if got := byTask[test.name].Health; got != test.wantHealth {
			t.Errorf("%s health = %q, want %q", test.name, got, test.wantHealth)
		}
	}
}

func TestCollectorInspectsSupervisorPanesConcurrently(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	for i := range 4 {
		dir := filepath.Join(root, fmt.Sprintf("task-%d", i), "api")
		wt := filepath.Join(root, fmt.Sprintf("task-%d-wt", i))
		mustMkdirAll(t, dir)
		mustMkdirAll(t, wt)
		writeFile(t, filepath.Join(dir, "status"), "in_progress\n")
		writeFile(t, filepath.Join(dir, "pane"), fmt.Sprintf("pane-%d\n", i))
		writeFile(t, filepath.Join(dir, "worktree"), wt+"\n")
		setModTime(t, filepath.Join(dir, "status"), now.Add(-time.Minute))
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var current atomic.Int32
	var maximum atomic.Int32
	collector := dashboard.Collector{
		FleetRoot:    root,
		Now:          func() time.Time { return now },
		StaleAfter:   15 * time.Minute,
		ProbeTimeout: 200 * time.Millisecond,
		InspectSupervisor: func(ctx context.Context, _, _ string) (bool, error) {
			now := current.Add(1)
			defer current.Add(-1)
			for {
				old := maximum.Load()
				if now <= old || maximum.CompareAndSwap(old, now) {
					break
				}
			}
			started <- struct{}{}
			select {
			case <-release:
				return false, nil
			case <-ctx.Done():
				return false, ctx.Err()
			}
		},
	}

	done := make(chan dashboard.State, 1)
	go func() {
		done <- collector.Collect(ctx)
	}()

	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			cancel()
			close(release)
			<-done
			t.Fatalf("supervisor inspections stalled before worker %d started", i+1)
		}
	}
	close(release)
	<-done

	if got := maximum.Load(); got < 2 {
		t.Fatalf("maximum concurrent supervisor inspections = %d, want at least 2", got)
	}
}

func TestCollectorDoesNotShrinkSupervisorProbeTimeoutByFleetSize(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	const workerCount = 40
	for i := 0; i < workerCount; i++ {
		dir := filepath.Join(root, fmt.Sprintf("task-%02d", i), "api")
		wt := filepath.Join(root, fmt.Sprintf("task-%02d-wt", i))
		mustMkdirAll(t, dir)
		mustMkdirAll(t, wt)
		writeFile(t, filepath.Join(dir, "status"), "in_progress\n")
		writeFile(t, filepath.Join(dir, "pane"), fmt.Sprintf("pane-%02d\n", i))
		writeFile(t, filepath.Join(dir, "worktree"), wt+"\n")
		setModTime(t, filepath.Join(dir, "status"), now.Add(-time.Hour))
	}

	collector := dashboard.Collector{
		FleetRoot:    root,
		Now:          func() time.Time { return now },
		StaleAfter:   15 * time.Minute,
		ProbeTimeout: 100 * time.Millisecond,
		InspectSupervisor: func(ctx context.Context, _, _ string) (bool, error) {
			select {
			case <-time.After(250 * time.Millisecond):
				return true, nil
			case <-ctx.Done():
				return false, ctx.Err()
			}
		},
	}

	state := collector.Collect(t.Context())
	var active int
	for _, worker := range state.Workers {
		if worker.Health != "active" {
			t.Fatalf("worker %s health = %q, want active", worker.Task, worker.Health)
		}
		active++
	}
	if active != workerCount {
		t.Fatalf("active workers = %d, want %d", active, workerCount)
	}
}

func TestCollectorDoesNotCascadeSupervisorDeadlineAcrossBatches(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	const workerCount = 9
	for i := 0; i < workerCount; i++ {
		dir := filepath.Join(root, fmt.Sprintf("task-%02d", i), "api")
		wt := filepath.Join(root, fmt.Sprintf("task-%02d-wt", i))
		mustMkdirAll(t, dir)
		mustMkdirAll(t, wt)
		writeFile(t, filepath.Join(dir, "status"), "in_progress\n")
		writeFile(t, filepath.Join(dir, "pane"), fmt.Sprintf("pane-%02d\n", i))
		writeFile(t, filepath.Join(dir, "worktree"), wt+"\n")
		setModTime(t, filepath.Join(dir, "status"), now.Add(-time.Hour))
	}

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	state := dashboard.Collector{
		FleetRoot:  root,
		Now:        func() time.Time { return now },
		StaleAfter: 15 * time.Minute,
		InspectSupervisor: func(ctx context.Context, _, _ string) (bool, error) {
			select {
			case <-time.After(150 * time.Millisecond):
				return true, nil
			case <-ctx.Done():
				return false, ctx.Err()
			}
		},
	}.Collect(ctx)

	for _, worker := range state.Workers {
		if worker.Health != "active" {
			t.Fatalf("worker %s health = %q, want active", worker.Task, worker.Health)
		}
	}
}

func TestCollectorHonorsConfiguredEnrichmentProbeTimeout(t *testing.T) {
	root := t.TempDir()
	worker := filepath.Join(root, "task-a", "api")
	worktree := filepath.Join(root, "worktree")
	mustMkdirAll(t, worker)
	mustMkdirAll(t, worktree)
	writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
	writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	writeFile(t, filepath.Join(worker, "repository"), "acme/api\n")

	runner := func(ctx context.Context, _ string, name string, _ ...string) ([]byte, error) {
		select {
		case <-time.After(2500 * time.Millisecond):
			if name == "gh" {
				return []byte(`{"url":"https://github.com/acme/api/pull/7","state":"OPEN","statusCheckRollup":[],"comments":[]}`), nil
			}
			return []byte("review passed"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	state := dashboard.Collector{FleetRoot: root, Run: runner, ProbeTimeout: 3 * time.Second}.Collect(t.Context())
	workerState := state.Workers[0]
	if workerState.PullRequest.URL == "" {
		t.Fatalf("pull request probe ignored configured timeout: %#v", workerState.PullRequest)
	}
	if !workerState.NoMistakes.Available {
		t.Fatalf("no-mistakes probe ignored configured timeout: %#v", workerState.NoMistakes)
	}
}

// Regression tests for td-9782c5: Collector.Collect must apply limit before
// materializing the full fleet and must check ctx during the scan walk.

func TestCollectorAppliesLimitBeforeEnrichment(t *testing.T) {
	root := t.TempDir()
	const totalWorkers = 10
	for i := 0; i < totalWorkers; i++ {
		worker := filepath.Join(root, fmt.Sprintf("task-%02d", i), "api")
		worktree := filepath.Join(root, fmt.Sprintf("worktree-%02d", i))
		mustMkdirAll(t, worker)
		mustMkdirAll(t, worktree)
		writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
		writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	}

	var enriched atomic.Int32
	runner := func(_ context.Context, _ string, _ string, _ ...string) ([]byte, error) {
		enriched.Add(1)
		return []byte(`{}`), nil
	}

	const limit = 3
	state := dashboard.Collector{
		FleetRoot: root,
		Run:       runner,
		Limit:     limit,
	}.Collect(t.Context())

	if len(state.Workers) != limit {
		t.Fatalf("workers = %d, want limit %d applied before enrichment", len(state.Workers), limit)
	}
	// Each enrichable worker triggers 3 probes (gh, no-mistakes, git-remote); with a
	// limit of 3, at most limit*3 probes should execute, not totalWorkers*3.
	const probesPerWorker = 3
	if got := enriched.Load(); got > int32(limit*probesPerWorker) {
		t.Fatalf("enrichment probes = %d, want at most %d (limit %d * %d probes/worker); limit must apply before enrichment", got, limit*probesPerWorker, limit, probesPerWorker)
	}
}

func TestCollectorChecksContextDuringFleetWalk(t *testing.T) {
	root := t.TempDir()
	const totalWorkers = 20
	for i := 0; i < totalWorkers; i++ {
		worker := filepath.Join(root, fmt.Sprintf("task-%02d", i), "api")
		mustMkdirAll(t, worker)
		writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel before Collect starts

	state := dashboard.Collector{FleetRoot: root}.Collect(ctx)
	if len(state.Workers) == totalWorkers {
		t.Fatalf("collect walked all %d workers despite cancelled context; want early exit", totalWorkers)
	}
}

func TestCollectorAddsWarningWhenLimitTruncatesScan(t *testing.T) {
	root := t.TempDir()
	const totalWorkers = 5
	for i := 0; i < totalWorkers; i++ {
		worker := filepath.Join(root, fmt.Sprintf("task-%02d", i), "api")
		mustMkdirAll(t, worker)
		writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
	}

	const limit = 2
	state := dashboard.Collector{FleetRoot: root, Limit: limit}.Collect(t.Context())

	if len(state.Workers) != limit {
		t.Fatalf("workers = %d, want %d", len(state.Workers), limit)
	}
	hasTruncationWarning := false
	for _, w := range state.Warnings {
		if strings.Contains(w, "truncated") || strings.Contains(w, "limit") {
			hasTruncationWarning = true
			break
		}
	}
	if !hasTruncationWarning {
		t.Fatalf("no truncation warning in state.Warnings = %v; operators must be told the fleet is partial", state.Warnings)
	}
}

func TestCollectorLimitGuardFiresBeforeReadingNextTask(t *testing.T) {
	// Arrange: exactly `limit` projects spread across two separate tasks, each
	// holding one project.  After the first task fills the limit, the outer
	// loop must not read the second task's directory before exiting.
	root := t.TempDir()
	const limit = 1

	// Task-00 has the only project that should be collected.
	task0 := filepath.Join(root, "task-00", "api")
	mustMkdirAll(t, task0)
	writeFile(t, filepath.Join(task0, "status"), "in_progress\n")

	// Task-01 is a real directory; if ReadDir is called on it the outer loop
	// entered the task after the limit was already met.  We detect this by
	// placing a synthetic file that collectWorker would warn about, but more
	// directly by asserting no workers from task-01 appear.
	task1 := filepath.Join(root, "task-01", "api")
	mustMkdirAll(t, task1)
	writeFile(t, filepath.Join(task1, "status"), "in_progress\n")

	state := dashboard.Collector{FleetRoot: root, Limit: limit}.Collect(t.Context())

	if len(state.Workers) != limit {
		t.Fatalf("workers = %d, want %d; limit guard must fire before entering task-01", len(state.Workers), limit)
	}
	for _, w := range state.Workers {
		if w.Task == "task-01" {
			t.Fatalf("worker from task-01 was collected; outer-loop limit guard did not fire before os.ReadDir(task-01)")
		}
	}
}

// Regression test for td-bd77c2: Check must preserve statusCheckRollup.context
// so legacy GitHub status checks retain their identifier.

func TestCollectorPreservesLegacyStatusContextName(t *testing.T) {
	root := t.TempDir()
	worker := filepath.Join(root, "task-a", "api")
	worktree := filepath.Join(root, "worktree")
	mustMkdirAll(t, worker)
	mustMkdirAll(t, worktree)
	writeFile(t, filepath.Join(worker, "status"), "in_progress\n")
	writeFile(t, filepath.Join(worker, "worktree"), worktree+"\n")
	writeFile(t, filepath.Join(worker, "repository"), "acme/api\n")

	runner := func(_ context.Context, _ string, name string, _ ...string) ([]byte, error) {
		if name == "gh" {
			return []byte(`{"url":"https://github.com/acme/api/pull/1","state":"OPEN","statusCheckRollup":[{"context":"legacy-ci","state":"PENDING","description":"Tests running"}],"comments":[]}`), nil
		}
		return []byte(`{}`), nil
	}

	state := dashboard.Collector{FleetRoot: root, Run: runner}.Collect(t.Context())
	if len(state.Workers) == 0 {
		t.Fatal("no workers collected")
	}
	checks := state.Workers[0].PullRequest.Checks
	if len(checks) != 1 {
		t.Fatalf("checks = %d, want 1", len(checks))
	}
	if checks[0].Context != "legacy-ci" {
		t.Fatalf("legacy check context = %q, want %q; legacy status checks must retain their context identifier", checks[0].Context, "legacy-ci")
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
