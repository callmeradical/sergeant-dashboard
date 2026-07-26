package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	maxScalarBytes       = 4096
	maxOperationalBytes  = 64 << 10
	maxConcurrentWorkers = 8
	maxEnrichmentBatches = 4
	maxEnrichmentTime    = 12 * time.Second
)

var processProbeSlots = make(chan struct{}, 2*maxConcurrentWorkers)

type Collector struct {
	FleetRoot         string
	StaleAfter        time.Duration
	Now               func() time.Time
	Run               Runner
	ProbeTimeout      time.Duration
	InspectSupervisor SupervisorInspector
}

type Runner func(ctx context.Context, dir, name string, args ...string) ([]byte, error)

// SupervisorInspector verifies whether a tmux pane is running the expected
// sgt-worker process for the given fleet state directory.
type SupervisorInspector func(ctx context.Context, pane, stateDir string) (bool, error)

type State struct {
	CollectedAt time.Time `json:"collectedAt"`
	Workers     []Worker  `json:"workers"`
	Warnings    []string  `json:"warnings"`
}

type Worker struct {
	Task        string        `json:"task"`
	Project     string        `json:"project"`
	Repository  string        `json:"repository,omitempty"`
	Status      string        `json:"status,omitempty"`
	Health      string        `json:"health"`
	Agent       string        `json:"agent,omitempty"`
	Branch      string        `json:"branch,omitempty"`
	TDTask      string        `json:"tdTask,omitempty"`
	TD          FileMetadata  `json:"td"`
	Worktree    string        `json:"worktree,omitempty"`
	UpdatedAt   time.Time     `json:"updatedAt,omitempty"`
	Message     FileMetadata  `json:"message"`
	Diagnostic  FileMetadata  `json:"diagnostic"`
	Log         FileMetadata  `json:"log"`
	Handoff     FileMetadata  `json:"handoff"`
	PullRequest PullRequest   `json:"pullRequest"`
	NoMistakes  ToolStatus    `json:"noMistakes"`
	Graphify    FileMetadata  `json:"graphify"`
	OCInject    AuditMetadata `json:"ocInject"`
	enrichable  bool
}

type FileMetadata struct {
	Present   bool      `json:"present"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
	Summary   string    `json:"summary,omitempty"`
	Status    string    `json:"status,omitempty"`
}

type PullRequest struct {
	URL      string    `json:"url,omitempty"`
	State    string    `json:"state,omitempty"`
	Checks   []Check   `json:"checks"`
	Comments []Comment `json:"comments"`
	Status   string    `json:"status,omitempty"`
}

type Comment struct {
	Author    string    `json:"author,omitempty"`
	Body      string    `json:"body,omitempty"`
	URL       string    `json:"url,omitempty"`
	CreatedAt time.Time `json:"createdAt,omitempty"`
}

type Check struct {
	Name       string `json:"name,omitempty"`
	Status     string `json:"status,omitempty"`
	Conclusion string `json:"conclusion,omitempty"`
	State      string `json:"state,omitempty"`
}

type ToolStatus struct {
	Available bool   `json:"available"`
	Phase     string `json:"phase,omitempty"`
	Summary   string `json:"summary,omitempty"`
	Status    string `json:"status,omitempty"`
}

type AuditMetadata struct {
	ResponsePending   bool      `json:"responsePending"`
	ResponsePendingAt time.Time `json:"responsePendingAt,omitempty"`
	ResponseAcked     bool      `json:"responseAcked"`
	ResponseAckedAt   time.Time `json:"responseAckedAt,omitempty"`
	UpdatedAt         time.Time `json:"updatedAt,omitempty"`
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
			worker, warnings := c.collectWorker(ctx, filepath.Join(c.FleetRoot, task.Name(), project.Name()), task.Name(), project.Name(), now, staleAfter)
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
	probeTimeout := c.ProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = 3 * time.Second
	}
	enrichmentTime := maxEnrichmentTime
	if probeTimeout <= maxEnrichmentTime/maxEnrichmentBatches {
		enrichmentTime = maxEnrichmentBatches * probeTimeout
	}
	probeCount := 0
	for index := range workers {
		if workers[index].enrichable {
			probeCount += 2
			if workers[index].Repository == "" {
				probeCount++
			}
			if tdTaskID.MatchString(workers[index].TDTask) {
				probeCount++
			}
		}
	}
	batchCount := (probeCount + cap(processProbeSlots) - 1) / cap(processProbeSlots)
	if batchCount > 0 {
		probeTimeout = min(probeTimeout, enrichmentTime/time.Duration(batchCount))
	}
	probeCollector := c
	probeCollector.ProbeTimeout = probeTimeout

	workerCount := min(maxConcurrentWorkers, len(workers))
	jobs := make(chan *Worker)
	var wait sync.WaitGroup
	wait.Add(workerCount)
	for range workerCount {
		go func() {
			defer wait.Done()
			for worker := range jobs {
				probeCollector.enrichWorker(ctx, worker)
			}
		}()
	}
	for index := range workers {
		select {
		case jobs <- &workers[index]:
		case <-ctx.Done():
			close(jobs)
			wait.Wait()
			return
		}
	}
	close(jobs)
	wait.Wait()
}

func (c Collector) enrichWorker(parent context.Context, worker *Worker) {
	worker.PullRequest.Checks = []Check{}
	pending := FileMetadata{}
	if worker.Worktree != "" {
		pending = fileMetadata(filepath.Join(filepath.Dir(worker.Worktree), "response_id"))
	}
	// Current Sergeant workers keep transport metadata beside their scalar state,
	// not necessarily in the worktree.
	if !pending.Present {
		pending = fileMetadata(filepath.Join(c.FleetRoot, worker.Task, worker.Project, "response_id"))
	}
	acked := fileMetadata(filepath.Join(c.FleetRoot, worker.Task, worker.Project, "response_ack"))
	worker.OCInject = AuditMetadata{
		ResponsePending:   pending.Present,
		ResponsePendingAt: pending.UpdatedAt,
		ResponseAcked:     acked.Present,
		ResponseAckedAt:   acked.UpdatedAt,
		UpdatedAt:         latestTime(pending.UpdatedAt, acked.UpdatedAt),
	}
	if !worker.enrichable {
		return
	}
	worker.Graphify = graphifyMetadata(worker.Worktree)

	run := c.Run
	if run == nil {
		run = runCommand
	}
	timeout := c.ProbeTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	pullRequest := make(chan PullRequest, 1)
	go func() {
		result := PullRequest{Checks: []Check{}, Comments: []Comment{}, Status: "unavailable"}
		output, err := runLimitedProbe(parent, timeout, run, worker.Worktree, "gh", "pr", "view", "--json", "url,state,statusCheckRollup,comments")
		if err == nil && len(output) > 1<<20 {
			result.Status = "oversized"
		} else if err == nil {
			var response struct {
				URL               string  `json:"url"`
				State             string  `json:"state"`
				StatusCheckRollup []Check `json:"statusCheckRollup"`
				Comments          []struct {
					Author struct {
						Login string `json:"login"`
					} `json:"author"`
					Body      string    `json:"body"`
					URL       string    `json:"url"`
					CreatedAt time.Time `json:"createdAt"`
				} `json:"comments"`
			}
			if json.Unmarshal(output, &response) == nil {
				if response.StatusCheckRollup == nil {
					response.StatusCheckRollup = []Check{}
				}
				comments := make([]Comment, 0, len(response.Comments))
				for _, comment := range response.Comments {
					comments = append(comments, Comment{Author: RedactText(comment.Author.Login), Body: RedactText(comment.Body), URL: RedactText(comment.URL), CreatedAt: comment.CreatedAt})
				}
				result = PullRequest{URL: response.URL, State: response.State, Checks: response.StatusCheckRollup, Comments: comments, Status: "available"}
			} else {
				result.Status = "corrupt"
			}
		}
		pullRequest <- result
	}()
	noMistakes := make(chan ToolStatus, 1)
	go func() {
		result := ToolStatus{Status: "unavailable"}
		output, err := runLimitedProbe(parent, timeout, run, worker.Worktree, "no-mistakes", "runs", "--limit", "1")
		if err == nil && len(output) > maxOperationalBytes {
			result.Status = "oversized"
		} else if err == nil {
			summary := strings.TrimSpace(RedactText(string(output)))
			if summary == "" {
				result.Status = "corrupt"
			} else {
				result = ToolStatus{Available: true, Phase: noMistakesPhase(output), Summary: summary, Status: "available"}
			}
		}
		noMistakes <- result
	}()
	td := make(chan FileMetadata, 1)
	go func() {
		if !tdTaskID.MatchString(worker.TDTask) {
			td <- FileMetadata{}
			return
		}
		output, err := runLimitedProbe(parent, timeout, run, worker.Worktree, "td", "context", worker.TDTask)
		if err != nil || len(output) > maxOperationalBytes {
			status := "unavailable"
			if err == nil {
				status = "oversized"
			}
			td <- FileMetadata{Present: true, Summary: "[content unavailable: " + status + "]", Status: status}
			return
		}
		summary := strings.TrimSpace(RedactText(string(output)))
		if summary == "" {
			td <- FileMetadata{Present: true, Summary: "[content unavailable: corrupt]", Status: "corrupt"}
			return
		}
		td <- FileMetadata{Present: true, Summary: summary, Status: "available"}
	}()
	repository := make(chan string, 1)
	go func() {
		if worker.Repository != "" {
			repository <- worker.Repository
			return
		}
		output, err := runLimitedProbe(parent, timeout, run, worker.Worktree, "git", "remote", "get-url", "origin")
		if err != nil || len(output) > maxScalarBytes {
			repository <- ""
			return
		}
		repository <- repositoryFromRemote(strings.TrimSpace(string(output)))
	}()
	worker.PullRequest = <-pullRequest
	worker.NoMistakes = <-noMistakes
	worker.TD = <-td
	worker.Repository = <-repository
}

func runLimitedProbe(parent context.Context, timeout time.Duration, run Runner, dir, name string, args ...string) ([]byte, error) {
	select {
	case processProbeSlots <- struct{}{}:
		defer func() { <-processProbeSlots }()
		ctx, cancel := context.WithTimeout(parent, timeout)
		defer cancel()
		return run(ctx, dir, name, args...)
	case <-parent.Done():
		return nil, parent.Err()
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
	output := boundedBuffer{remaining: (1 << 20) + 1}
	command.Stdout = &output
	if err := command.Run(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

type boundedBuffer struct {
	bytes.Buffer
	remaining int
}

func (buffer *boundedBuffer) Write(data []byte) (int, error) {
	written := len(data)
	if len(data) > buffer.remaining {
		data = data[:buffer.remaining]
	}
	if len(data) > 0 {
		_, _ = buffer.Buffer.Write(data)
		buffer.remaining -= len(data)
	}
	return written, nil
}

func (c Collector) collectWorker(ctx context.Context, dir, task, project string, now time.Time, staleAfter time.Duration) (Worker, []string) {
	worker := Worker{Task: task, Project: project, Health: "orphaned"}
	warnings := []string{}
	var err error
	worker.Status, worker.UpdatedAt, err = readScalarWithTime(filepath.Join(dir, "status"))
	if err != nil || !isLifecycle(worker.Status) {
		warnings = append(warnings, fmt.Sprintf("worker %s/%s has invalid status", task, project))
		worker.Status = "unknown"
		worker.Health = "unknown"
	}
	worker.Agent, _, _ = readScalarWithTime(filepath.Join(dir, "agent"))
	worker.Branch, _, _ = readScalarWithTime(filepath.Join(dir, "branch"))
	worker.Repository, _, _ = readScalarWithTime(filepath.Join(dir, "repository"))
	worker.TDTask, _, _ = readScalarWithTime(filepath.Join(dir, "td_task"))
	worker.Worktree, _, _ = readScalarWithTime(filepath.Join(dir, "worktree"))
	worker.Message = operationalFile(filepath.Join(dir, "message"))
	worker.Diagnostic = operationalFile(filepath.Join(dir, "diagnostic"))
	worker.Log = operationalFile(filepath.Join(dir, "worker.log"))
	worker.Handoff = operationalFile(filepath.Join(dir, "handoff"))
	if worker.Worktree != "" {
		if info, statErr := os.Stat(worker.Worktree); statErr == nil && info.IsDir() {
			worker.enrichable = true
		}
	}

	if worker.Status == "unknown" {
		return worker, warnings
	}
	if worker.Status == "orphaned" {
		return worker, warnings
	}
	if isTerminal(worker.Status) {
		if worker.enrichable {
			worker.Health = "complete"
		} else {
			worker.Health = "recycled"
		}
		return worker, warnings
	}
	if !worker.enrichable {
		return worker, warnings
	}

	activityAt := latestTime(worker.UpdatedAt, latestTime(worker.Log.UpdatedAt, latestTime(worker.Message.UpdatedAt, worker.Diagnostic.UpdatedAt)))

	// Classify health using supervisor identity when a pane file is present.
	pane, _, _ := readScalarWithTime(filepath.Join(dir, "pane"))
	if pane != "" {
		inspect := c.InspectSupervisor
		if inspect == nil {
			inspect = inspectSupervisor
		}
		live, err := inspect(ctx, pane, dir)
		if err != nil {
			// Inspection unavailable; fall back to timestamp-based classification.
			worker.Health = ageHealth(activityAt, now, staleAfter)
			return worker, warnings
		}
		if live {
			worker.Health = "active"
			return worker, warnings
		}
		// Dead pane: stale if timestamp is old, otherwise orphaned.
		worker.Health = ageHealth(activityAt, now, staleAfter)
		return worker, warnings
	}

	// No pane file: classify by status timestamp alone.
	if !activityAt.IsZero() && now.Sub(activityAt) > staleAfter {
		worker.Health = "stale"
	} else if worker.Status != "" {
		worker.Health = "active"
	}
	return worker, warnings
}

// ageHealth returns "stale" if activityAt is more than staleAfter in the past,
// and "orphaned" otherwise. Used when a supervisor pane is unavailable or dead.
func ageHealth(activityAt, now time.Time, staleAfter time.Duration) string {
	if !activityAt.IsZero() && now.Sub(activityAt) > staleAfter {
		return "stale"
	}
	return "orphaned"
}

// inspectSupervisor is the real supervisor inspector that queries tmux.
func inspectSupervisor(ctx context.Context, pane, stateDir string) (bool, error) {
	output, err := runCommand(ctx, "", "tmux", "display-message", "-p", "-t", pane, "#{pane_dead}|#{pane_start_command}")
	if err != nil {
		return false, err
	}
	return ParseSupervisorEvidence(strings.TrimSpace(string(output)), stateDir)
}

// ParseSupervisorEvidence parses tmux pane_dead and pane_start_command output
// to determine whether the pane is running an sgt-worker process for stateDir.
// output has the form "<0|1>|<pane_start_command>".
func ParseSupervisorEvidence(output, stateDir string) (bool, error) {
	dead, command, found := strings.Cut(output, "|")
	if !found || (dead != "0" && dead != "1") {
		return false, fmt.Errorf("invalid supervisor evidence")
	}
	if dead != "0" {
		return false, nil
	}
	fields, err := shellFields(command)
	if err != nil {
		return false, err
	}
	// sgt-worker <stateDir> [<worktree> [<agent> [<initial_message>]]]
	if len(fields) >= 2 && filepath.Base(fields[0]) == "sgt-worker" && fields[1] == stateDir {
		return true, nil
	}
	return false, nil
}

// shellFields splits a POSIX-shell-style command string into fields,
// handling single quotes, double quotes, and backslash escapes.
func shellFields(command string) ([]string, error) {
	fields := []string{}
	var field strings.Builder
	quote := rune(0)
	escaped := false
	flush := func() {
		if field.Len() > 0 {
			fields = append(fields, field.String())
			field.Reset()
		}
	}
	for _, character := range command {
		if escaped {
			field.WriteRune(character)
			escaped = false
			continue
		}
		if character == '\\' && quote != '\'' {
			escaped = true
			continue
		}
		if quote != 0 {
			if character == quote {
				quote = 0
			} else {
				field.WriteRune(character)
			}
			continue
		}
		if character == '\'' || character == '"' {
			quote = character
			continue
		}
		if character == ' ' || character == '\t' || character == '\n' {
			flush()
			continue
		}
		field.WriteRune(character)
	}
	if escaped || quote != 0 {
		return nil, fmt.Errorf("invalid supervisor command")
	}
	flush()
	return fields, nil
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
	report := operationalFile(filepath.Join(worktree, "graphify-out", "GRAPH_REPORT.md"))
	pending := fileMetadata(filepath.Join(worktree, "graphify-out", ".needs_update"))
	if pending.Present {
		if report.Summary == "" {
			report.Summary = "update pending"
		} else {
			report.Summary = "update pending\n" + report.Summary
		}
		report.Present = true
		report.UpdatedAt = latestTime(report.UpdatedAt, pending.UpdatedAt)
	}
	if !report.Present {
		report.Status = "missing"
	}
	return report
}

func isTerminal(status string) bool {
	return status == "done" || status == "failed" || strings.HasPrefix(status, "failed:")
}

func isLifecycle(status string) bool {
	return status == "in_progress" || status == "needs_input" || status == "blocked" || status == "orphaned" || isTerminal(status)
}

func latestTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}
