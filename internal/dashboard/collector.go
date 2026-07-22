package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const maxScalarBytes = 4096

type Collector struct {
	FleetRoot    string
	StaleAfter   time.Duration
	Now          func() time.Time
	Run          Runner
	ProbeTimeout time.Duration
}

type Runner func(ctx context.Context, dir, name string, args ...string) ([]byte, error)

type State struct {
	CollectedAt time.Time `json:"collectedAt"`
	Workers     []Worker  `json:"workers"`
	Warnings    []string  `json:"warnings"`
}

type Worker struct {
	Task        string        `json:"task"`
	Project     string        `json:"project"`
	Status      string        `json:"status,omitempty"`
	Health      string        `json:"health"`
	Agent       string        `json:"agent,omitempty"`
	Branch      string        `json:"branch,omitempty"`
	TDTask      string        `json:"tdTask,omitempty"`
	Worktree    string        `json:"worktree,omitempty"`
	UpdatedAt   time.Time     `json:"updatedAt,omitempty"`
	Message     FileMetadata  `json:"message"`
	PullRequest PullRequest   `json:"pullRequest"`
	NoMistakes  ToolStatus    `json:"noMistakes"`
	Graphify    FileMetadata  `json:"graphify"`
	OCInject    AuditMetadata `json:"ocInject"`
}

type FileMetadata struct {
	Present   bool      `json:"present"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
	Summary   string    `json:"summary,omitempty"`
}

type PullRequest struct {
	URL    string  `json:"url,omitempty"`
	State  string  `json:"state,omitempty"`
	Checks []Check `json:"checks"`
}

type Check struct {
	Name       string `json:"name,omitempty"`
	Status     string `json:"status,omitempty"`
	Conclusion string `json:"conclusion,omitempty"`
}

type ToolStatus struct {
	Available bool   `json:"available"`
	Phase     string `json:"phase,omitempty"`
}

type AuditMetadata struct {
	ResponsePending bool      `json:"responsePending"`
	ResponseAcked   bool      `json:"responseAcked"`
	UpdatedAt       time.Time `json:"updatedAt,omitempty"`
}

func (c Collector) Collect(ctx context.Context) State {
	now := time.Now().UTC()
	if c.Now != nil {
		now = c.Now().UTC()
	}
	staleAfter := c.StaleAfter
	if staleAfter <= 0 {
		staleAfter = 30 * time.Minute
	}
	state := State{CollectedAt: now, Workers: []Worker{}, Warnings: []string{}}

	tasks, err := os.ReadDir(c.FleetRoot)
	if err != nil {
		state.Warnings = append(state.Warnings, "fleet source unavailable")
		return state
	}
	for _, task := range tasks {
		if !task.IsDir() {
			continue
		}
		projects, err := os.ReadDir(filepath.Join(c.FleetRoot, task.Name()))
		if err != nil {
			state.Warnings = append(state.Warnings, fmt.Sprintf("task %s unavailable", task.Name()))
			continue
		}
		for _, project := range projects {
			if !project.IsDir() {
				continue
			}
			worker, warnings := collectWorker(filepath.Join(c.FleetRoot, task.Name(), project.Name()), task.Name(), project.Name(), now, staleAfter)
			state.Workers = append(state.Workers, worker)
			state.Warnings = append(state.Warnings, warnings...)
		}
	}
	sort.Slice(state.Workers, func(i, j int) bool {
		if state.Workers[i].Task == state.Workers[j].Task {
			return state.Workers[i].Project < state.Workers[j].Project
		}
		return state.Workers[i].Task < state.Workers[j].Task
	})
	c.enrichWorkers(ctx, state.Workers)
	return state
}

func (c Collector) enrichWorkers(ctx context.Context, workers []Worker) {
	var wait sync.WaitGroup
	limit := make(chan struct{}, 8)
	for index := range workers {
		wait.Add(1)
		go func(worker *Worker) {
			defer wait.Done()
			select {
			case limit <- struct{}{}:
				defer func() { <-limit }()
				c.enrichWorker(ctx, worker)
			case <-ctx.Done():
			}
		}(&workers[index])
	}
	wait.Wait()
}

func (c Collector) enrichWorker(parent context.Context, worker *Worker) {
	worker.PullRequest.Checks = []Check{}
	if worker.Worktree == "" {
		return
	}
	worker.Graphify = graphifyMetadata(worker.Worktree)
	pending := fileMetadata(filepath.Join(filepath.Dir(worker.Worktree), "response_id"))
	// Current Sergeant workers keep transport metadata beside their scalar state,
	// not necessarily in the worktree.
	if !pending.Present {
		pending = fileMetadata(filepath.Join(c.FleetRoot, worker.Task, worker.Project, "response_id"))
	}
	acked := fileMetadata(filepath.Join(c.FleetRoot, worker.Task, worker.Project, "response_ack"))
	worker.OCInject = AuditMetadata{ResponsePending: pending.Present, ResponseAcked: acked.Present, UpdatedAt: latestTime(pending.UpdatedAt, acked.UpdatedAt)}

	run := c.Run
	if run == nil {
		run = runCommand
	}
	timeout := c.ProbeTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	githubCtx, cancelGitHub := context.WithTimeout(parent, timeout)
	output, err := run(githubCtx, worker.Worktree, "gh", "pr", "view", "--json", "url,state,statusCheckRollup")
	cancelGitHub()
	if err == nil && len(output) <= 1<<20 {
		var response struct {
			URL               string  `json:"url"`
			State             string  `json:"state"`
			StatusCheckRollup []Check `json:"statusCheckRollup"`
		}
		if json.Unmarshal(output, &response) == nil {
			if response.StatusCheckRollup == nil {
				response.StatusCheckRollup = []Check{}
			}
			worker.PullRequest = PullRequest{URL: response.URL, State: response.State, Checks: response.StatusCheckRollup}
		}
	}
	noMistakesCtx, cancelNoMistakes := context.WithTimeout(parent, timeout)
	noMistakesOutput, err := run(noMistakesCtx, worker.Worktree, "no-mistakes", "runs", "--limit", "1")
	cancelNoMistakes()
	if err == nil {
		worker.NoMistakes = ToolStatus{Available: true, Phase: noMistakesPhase(noMistakesOutput)}
	}
}

func noMistakesPhase(output []byte) string {
	if len(output) > maxScalarBytes {
		return ""
	}
	fields := strings.FieldsFunc(strings.ToLower(string(output)), func(character rune) bool {
		return character < 'a' || character > 'z'
	})
	for _, field := range fields {
		switch field {
		case "intent", "rebase", "review", "test", "document", "lint", "push", "pr", "ci":
			return field
		}
	}
	return ""
}

func runCommand(ctx context.Context, dir, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = dir
	return command.Output()
}

func collectWorker(dir, task, project string, now time.Time, staleAfter time.Duration) (Worker, []string) {
	worker := Worker{Task: task, Project: project, Health: "orphaned"}
	warnings := []string{}
	var err error
	worker.Status, worker.UpdatedAt, err = readScalarWithTime(filepath.Join(dir, "status"))
	if err != nil {
		warnings = append(warnings, fmt.Sprintf("worker %s/%s has invalid status", task, project))
	}
	worker.Agent, _, _ = readScalarWithTime(filepath.Join(dir, "agent"))
	worker.Branch, _, _ = readScalarWithTime(filepath.Join(dir, "branch"))
	worker.TDTask, _, _ = readScalarWithTime(filepath.Join(dir, "td_task"))
	worker.Worktree, _, _ = readScalarWithTime(filepath.Join(dir, "worktree"))
	worker.Message = fileMetadata(filepath.Join(dir, "message"))

	if worker.Status == "orphaned" {
		return worker, warnings
	}
	if worker.Worktree != "" {
		if info, statErr := os.Stat(worker.Worktree); statErr != nil || !info.IsDir() {
			return worker, warnings
		}
	}
	if isTerminal(worker.Status) {
		worker.Health = "complete"
	} else if !worker.UpdatedAt.IsZero() && now.Sub(worker.UpdatedAt) > staleAfter {
		worker.Health = "stale"
	} else if worker.Status != "" {
		worker.Health = "active"
	}
	return worker, warnings
}

func readScalarWithTime(path string) (string, time.Time, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", time.Time{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxScalarBytes+1))
	if err != nil || len(data) > maxScalarBytes {
		return "", time.Time{}, fmt.Errorf("invalid scalar")
	}
	info, err := file.Stat()
	if err != nil {
		return "", time.Time{}, err
	}
	return strings.TrimSpace(string(data)), info.ModTime().UTC(), nil
}

func fileMetadata(path string) FileMetadata {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return FileMetadata{}
	}
	return FileMetadata{Present: true, UpdatedAt: info.ModTime().UTC()}
}

func graphifyMetadata(worktree string) FileMetadata {
	report := fileMetadata(filepath.Join(worktree, "graphify-out", "GRAPH_REPORT.md"))
	pending := fileMetadata(filepath.Join(worktree, "graphify-out", ".needs_update"))
	if pending.Present {
		return FileMetadata{Present: true, UpdatedAt: latestTime(report.UpdatedAt, pending.UpdatedAt), Summary: "update pending"}
	}
	if report.Present {
		report.Summary = "ready"
	}
	return report
}

func isTerminal(status string) bool {
	return status == "done" || status == "failed" || strings.HasPrefix(status, "failed:")
}

func latestTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}

var (
	sensitiveKey = regexp.MustCompile(`(?i)(authorization|body|cookie|credential|env|message|password|prompt|secret|token)`)
	secretValue  = regexp.MustCompile(`(?i)(bearer\s+)[^\s]+|(password\s*=\s*)[^\s]+|gh[pousr]_[A-Za-z0-9_]{20,}`)
)

func RedactMetadata(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		redacted := make(map[string]any, len(typed))
		for key, item := range typed {
			if sensitiveKey.MatchString(key) {
				redacted[key] = "[REDACTED]"
			} else {
				redacted[key] = RedactMetadata(item)
			}
		}
		return redacted
	case []any:
		redacted := make([]any, len(typed))
		for index, item := range typed {
			redacted[index] = RedactMetadata(item)
		}
		return redacted
	case string:
		return redactString(typed)
	default:
		return value
	}
}

func redactString(value string) string {
	return secretValue.ReplaceAllStringFunc(value, func(match string) string {
		lower := strings.ToLower(match)
		if strings.HasPrefix(lower, "bearer ") {
			return "Bearer [REDACTED]"
		}
		if index := strings.Index(match, "="); index >= 0 {
			return match[:index+1] + "[REDACTED]"
		}
		return "[REDACTED]"
	})
}

func MustJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(data)
}
