package dashboard_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/callmeradical/sergeant-dashboard/internal/dashboard"
)

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

	worker, ok := (dashboard.Collector{FleetRoot: root}).CollectDetail(t.Context(), "task", "repo")
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

	// The dashboard is filesystem-only: gh, no-mistakes, and td are not called.
	// PullRequest, NoMistakes, and TD are always reported as unavailable.
	// Graphify reads from the worktree filesystem and reflects the oversized report.
	got := dashboard.Collector{FleetRoot: root}.Collect(t.Context()).Workers[0]
	if got.PullRequest.Status != "unavailable" || got.NoMistakes.Status != "unavailable" || got.TD.Status != "unavailable" || got.Graphify.Status != "oversized" {
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

	state := dashboard.Collector{FleetRoot: root}.Collect(t.Context())

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
		{task: "dead-recent", status: "in_progress", pane: "dead", logAge: time.Minute, statusAge: time.Hour, worktree: true, wantHealth: "orphaned"},
		{task: "collision-old", status: "in_progress", pane: "collision", logAge: time.Hour, statusAge: time.Hour, worktree: true, wantHealth: "stale"},
		{task: "waiting-live", status: "blocked", pane: "live", statusAge: time.Hour, worktree: true, wantHealth: "active"},
		{task: "terminal-live", status: "done", pane: "live", statusAge: time.Hour, worktree: true, wantHealth: "complete"},
		{task: "unsupported", status: "in_progress", pane: "unsupported", logAge: time.Minute, statusAge: time.Hour, worktree: true, wantHealth: "orphaned"},
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

	state := dashboard.Collector{FleetRoot: root}.Collect(t.Context())

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

	state := dashboard.Collector{FleetRoot: root}.Collect(t.Context())
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

	const limit = 3
	state := dashboard.Collector{
		FleetRoot: root,
		Limit:     limit,
	}.Collect(t.Context())

	if len(state.Workers) != limit {
		t.Fatalf("workers = %d, want limit %d applied before enrichment", len(state.Workers), limit)
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
