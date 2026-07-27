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

	"gopkg.in/yaml.v3"
)

const (
	maxScalarBytes         = 4096
	maxOperationalBytes    = 64 << 10
	maxConcurrentWorkers   = 8
	maxEnrichmentBatches   = 4
	maxEnrichmentTime      = 12 * time.Second
	supervisorProbeTimeout = 2 * time.Second
)

var processProbeSlots = make(chan struct{}, 2*maxConcurrentWorkers)

type Collector struct {
	FleetRoot         string
	ConfigRoot        string
	StaleAfter        time.Duration
	Now               func() time.Time
	Run               Runner
	ProbeTimeout      time.Duration
	InspectSupervisor SupervisorInspector
	// Limit caps the number of workers materialised during the fleet scan.
	// Zero or negative means no limit.
	Limit int
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
	Task           string        `json:"task"`
	Title          string        `json:"title,omitempty"`
	Project        string        `json:"project"`
	Repository     string        `json:"repository,omitempty"`
	Status         string        `json:"status,omitempty"`
	Health         string        `json:"health"`
	Agent          string        `json:"agent,omitempty"`
	Branch         string        `json:"branch,omitempty"`
	TDTask         string        `json:"tdTask,omitempty"`
	TD             FileMetadata  `json:"td"`
	Worktree       string        `json:"worktree,omitempty"`
	UpdatedAt      time.Time     `json:"updatedAt,omitempty"`
	Message        FileMetadata  `json:"message"`
	Diagnostic     FileMetadata  `json:"diagnostic"`
	Log            FileMetadata  `json:"log"`
	Handoff        FileMetadata  `json:"handoff"`
	PullRequest    PullRequest   `json:"pullRequest"`
	NoMistakes     ToolStatus    `json:"noMistakes"`
	Graphify       FileMetadata  `json:"graphify"`
	OCInject       AuditMetadata `json:"ocInject"`
	enrichable     bool
	fleetProject   string
	summaryError   string
	supervisorPane string
	stateDir       string
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
	Context    string `json:"context,omitempty"`
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
	return c.collect(ctx, true)
}

func (c Collector) CollectSummary(ctx context.Context) State {
	return c.collect(ctx, false)
}

func (c Collector) collect(ctx context.Context, detail bool) State {
	now := time.Now().UTC()
	if c.Now != nil {
		now = c.Now().UTC()
	}
	staleAfter := c.StaleAfter
	if staleAfter <= 0 {
		staleAfter = 30 * time.Minute
	}
	state := State{CollectedAt: now, Workers: []Worker{}, Warnings: []string{}}

	if ctx.Err() != nil {
		state.Warnings = append(state.Warnings, "fleet collection canceled")
		return state
	}

	tasks, err := os.ReadDir(c.FleetRoot)
	if err != nil {
		state.Warnings = append(state.Warnings, "fleet source unavailable")
		return state
	}
	truncatedByLimit := false
outer:
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			break outer
		default:
		}
		if !task.IsDir() {
			continue
		}
		// Check the limit before reading the next task's directory so a
		// completed scan does not issue unnecessary ReadDir calls.
		if c.Limit > 0 && len(state.Workers) >= c.Limit {
			truncatedByLimit = true
			break outer
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
			select {
			case <-ctx.Done():
				break outer
			default:
			}
			if c.Limit > 0 && len(state.Workers) >= c.Limit {
				truncatedByLimit = true
				break outer
			}
			if len(state.Workers) >= maxProjectedWorkers {
				state.Warnings = append(state.Warnings, "fleet collection truncated at safety limit")
				break outer
			}
			worker, warnings := c.collectWorker(ctx, filepath.Join(c.FleetRoot, task.Name(), project.Name()), task.Name(), project.Name(), now, staleAfter)
			worker.fleetProject = project.Name()
			if c.ConfigRoot != "" {
				worker.Project, worker.Repository, worker.Title = c.configuredIdentity(ctx, task.Name(), project.Name())
			}
			state.Workers = append(state.Workers, worker)
			state.Warnings = append(state.Warnings, warnings...)
		}
	}
	if truncatedByLimit {
		state.Warnings = append(state.Warnings, fmt.Sprintf("worker scan truncated at configured limit %d; fleet may be partial", c.Limit))
	}
	sort.Slice(state.Workers, func(i, j int) bool {
		if state.Workers[i].Task == state.Workers[j].Task {
			return state.Workers[i].Project < state.Workers[j].Project
		}
		return state.Workers[i].Task < state.Workers[j].Task
	})
	c.inspectWorkers(ctx, now, staleAfter, state.Workers)
	if detail {
		c.enrichWorkers(ctx, state.Workers)
	} else {
		c.enrichSummaryWorkers(ctx, state.Workers)
		for _, worker := range state.Workers {
			if worker.summaryError != "" {
				state.Warnings = append(state.Warnings, worker.summaryError)
			}
		}
	}
	return state
}

func (c Collector) CollectDetail(ctx context.Context, task, repository string) (Worker, bool) {
	if ctx.Err() != nil || !configIdentifier.MatchString(task) || !configIdentifier.MatchString(repository) {
		return Worker{}, false
	}
	now := time.Now().UTC()
	if c.Now != nil {
		now = c.Now().UTC()
	}
	staleAfter := c.StaleAfter
	if staleAfter <= 0 {
		staleAfter = 30 * time.Minute
	}
	root, err := os.OpenRoot(c.FleetRoot)
	if err != nil {
		return Worker{}, false
	}
	defer root.Close()
	relative := filepath.Join(task, repository)
	directory, _, err := openRootDirectory(root, relative)
	if err != nil {
		return Worker{}, false
	}
	directory.Close()
	dir := filepath.Join(c.FleetRoot, relative)
	worker, _ := collectWorkerRoot(root, relative, task, repository, now, staleAfter, true)
	worker.fleetProject = repository
	if c.ConfigRoot != "" {
		worker.Project, worker.Repository, worker.Title = c.configuredIdentity(ctx, task, repository)
	}
	worker.OCInject = collectAuditMetadata(root, relative, worker.Worktree)
	worker.stateDir = dir
	pane, _, _ := readRootScalarWithTime(root, filepath.Join(relative, "pane"))
	if pane != "" {
		worker.supervisorPane = pane
	}
	inspect := c.InspectSupervisor
	if inspect == nil {
		inspect = inspectSupervisor
	}
	if worker.supervisorPane != "" {
		probeTimeout := supervisorProbeTimeout
		if c.ProbeTimeout > 0 {
			probeTimeout = c.ProbeTimeout
		}
		inspectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), probeTimeout)
		live, inspectErr := inspect(inspectCtx, worker.supervisorPane, worker.stateDir)
		cancel()
		activityAt := latestTime(worker.UpdatedAt, latestTime(worker.Log.UpdatedAt, latestTime(worker.Message.UpdatedAt, worker.Diagnostic.UpdatedAt)))
		if inspectErr == nil && live {
			worker.Health = "active"
		} else {
			worker.Health = ageHealth(activityAt, now, staleAfter)
		}
	}
	c.enrichWorker(ctx, &worker)
	return worker, ctx.Err() == nil
}

func (c Collector) inspectWorkers(ctx context.Context, now time.Time, staleAfter time.Duration, workers []Worker) {
	inspectionCount := 0
	for index := range workers {
		if workers[index].supervisorPane != "" {
			inspectionCount++
		}
	}
	if inspectionCount == 0 {
		return
	}

	inspect := c.InspectSupervisor
	if inspect == nil {
		inspect = inspectSupervisor
	}
	probeTimeout := supervisorProbeTimeout

	workerCount := min(maxConcurrentWorkers, inspectionCount)
	jobs := make(chan *Worker)
	var wait sync.WaitGroup
	wait.Add(workerCount)
	for range workerCount {
		go func() {
			defer wait.Done()
			for worker := range jobs {
				inspectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), probeTimeout)
				live, err := inspect(inspectCtx, worker.supervisorPane, worker.stateDir)
				cancel()
				activityAt := latestTime(worker.UpdatedAt, latestTime(worker.Log.UpdatedAt, latestTime(worker.Message.UpdatedAt, worker.Diagnostic.UpdatedAt)))
				if err == nil && live {
					worker.Health = "active"
				} else {
					worker.Health = ageHealth(activityAt, now, staleAfter)
				}
			}
		}()
	}
	for index := range workers {
		if workers[index].supervisorPane == "" {
			continue
		}
		select {
		case jobs <- &workers[index]:
		case <-ctx.Done():
			activityAt := latestTime(workers[index].UpdatedAt, latestTime(workers[index].Log.UpdatedAt, latestTime(workers[index].Message.UpdatedAt, workers[index].Diagnostic.UpdatedAt)))
			workers[index].Health = ageHealth(activityAt, now, staleAfter)
			for remaining := index + 1; remaining < len(workers); remaining++ {
				if workers[remaining].supervisorPane != "" {
					activityAt := latestTime(workers[remaining].UpdatedAt, latestTime(workers[remaining].Log.UpdatedAt, latestTime(workers[remaining].Message.UpdatedAt, workers[remaining].Diagnostic.UpdatedAt)))
					workers[remaining].Health = ageHealth(activityAt, now, staleAfter)
				}
			}
			close(jobs)
			wait.Wait()
			return
		}
	}
	close(jobs)
	wait.Wait()
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

	// Semaphore of size maxConcurrentWorkers: the dispatcher acquires one slot
	// before launching each goroutine and the goroutine releases it on return.
	// This bounds concurrent goroutines to maxConcurrentWorkers (not fleet size),
	// fixing both td-ab7c04 and td-577246.
	sem := make(chan struct{}, maxConcurrentWorkers)
	var wg sync.WaitGroup
dispatch:
	for index := range workers {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break dispatch
		}
		wg.Add(1)
		w := &workers[index]
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			probeCollector.enrichWorker(ctx, w)
		}()
	}
	wg.Wait()
}

func (c Collector) enrichSummaryWorkers(ctx context.Context, workers []Worker) {
	enrichable := 0
	for index := range workers {
		if workers[index].enrichable {
			enrichable++
		}
	}
	if enrichable == 0 {
		return
	}
	timeout := c.ProbeTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	batches := (enrichable + maxConcurrentWorkers - 1) / maxConcurrentWorkers
	enrichmentTime := min(time.Duration(batches)*timeout, maxEnrichmentTime)
	probeCtx, cancel := context.WithTimeout(ctx, enrichmentTime)
	defer cancel()
	jobs := make(chan *Worker)
	workerCount := min(enrichable, maxConcurrentWorkers)
	var wait sync.WaitGroup
	wait.Add(workerCount)
	for range workerCount {
		go func() {
			defer wait.Done()
			for worker := range jobs {
				c.enrichSummaryWorker(probeCtx, worker, timeout)
			}
		}()
	}
	for index := range workers {
		if !workers[index].enrichable {
			continue
		}
		select {
		case jobs <- &workers[index]:
		case <-probeCtx.Done():
			close(jobs)
			wait.Wait()
			return
		}
	}
	close(jobs)
	wait.Wait()
}

func (c Collector) enrichSummaryWorker(ctx context.Context, worker *Worker, timeout time.Duration) {
	run := c.Run
	if run == nil {
		run = runCommand
	}
	output, err := runLimitedProbe(ctx, timeout, run, worker.Worktree, "gh", "pr", "view", "--json", "state")
	if err != nil {
		worker.summaryError = fmt.Sprintf("worker %s/%s summary probe failed", worker.Task, worker.fleetProject)
		return
	}
	if len(output) > maxScalarBytes {
		worker.summaryError = fmt.Sprintf("worker %s/%s summary probe failed: oversized response", worker.Task, worker.fleetProject)
		return
	}
	var response struct {
		State string `json:"state"`
	}
	if json.Unmarshal(output, &response) != nil {
		worker.summaryError = fmt.Sprintf("worker %s/%s summary probe failed: corrupt response", worker.Task, worker.fleetProject)
		return
	}
	worker.PullRequest.State = RedactText(response.State)
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
		worker.PullRequest.Status = "unavailable"
		worker.NoMistakes.Status = "unavailable"
		worker.Graphify.Status = "missing"
		if worker.TDTask != "" {
			worker.TD = FileMetadata{Present: true, Summary: "[content unavailable: unavailable]", Status: "unavailable"}
		}
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

type projectConfig struct {
	Name  string `yaml:"name"`
	Repos []struct {
		Name string `yaml:"name"`
		Path string `yaml:"path"`
	} `yaml:"repos"`
}

// configuredIdentity reads project, repository, and title from the fleet brief
// and config root. Returns ("unavailable", "unavailable", title) on error.
func (c Collector) configuredIdentity(ctx context.Context, task, repository string) (string, string, string) {
	if c.ConfigRoot == "" {
		return repository, "", ""
	}
	fleetRoot, err := os.OpenRoot(c.FleetRoot)
	if err != nil {
		return "unavailable", "unavailable", ""
	}
	defer fleetRoot.Close()
	brief, err := readRootRegularFileBounded(ctx, fleetRoot, filepath.Join(task, "brief.md"), maxScalarBytes)
	if err != nil {
		return "unavailable", "unavailable", ""
	}
	projectID, projectOK := briefValue(brief, "Project")
	title, titleOK := briefValue(brief, "Brief")
	if !projectOK || !titleOK || !configIdentifier.MatchString(projectID) {
		return "unavailable", "unavailable", title
	}
	configRoot, err := os.OpenRoot(c.ConfigRoot)
	if err != nil {
		return "unavailable", "unavailable", title
	}
	defer configRoot.Close()
	configData, err := readRootRegularFileBounded(ctx, configRoot, projectID+".yaml", maxOperationalBytes)
	if err != nil {
		return "unavailable", "unavailable", title
	}
	var config projectConfig
	if yaml.Unmarshal(configData, &config) != nil || config.Name != projectID {
		return "unavailable", "unavailable", title
	}
	matched := false
	seen := make(map[string]struct{}, len(config.Repos))
	for _, repo := range config.Repos {
		if !configIdentifier.MatchString(repo.Name) || strings.TrimSpace(repo.Path) == "" {
			return "unavailable", "unavailable", title
		}
		if _, duplicate := seen[repo.Name]; duplicate {
			return "unavailable", "unavailable", title
		}
		seen[repo.Name] = struct{}{}
		matched = matched || repo.Name == repository
	}
	if !matched {
		return "unavailable", "unavailable", title
	}
	return config.Name, repository, title
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

	pane, _, _ := readScalarWithTime(filepath.Join(dir, "pane"))
	if pane != "" {
		worker.Health = ageHealth(activityAt, now, staleAfter)
		worker.supervisorPane = pane
		worker.stateDir = dir
		return worker, warnings
	}

	if !activityAt.IsZero() && now.Sub(activityAt) > staleAfter {
		worker.Health = "stale"
	} else if worker.Status != "" {
		worker.Health = "active"
	}
	return worker, warnings
}

// collectWorkerRoot collects a worker using root-sandboxed file access.
// Used for CollectDetail to prevent path traversal.
func collectWorkerRoot(root *os.Root, dir, task, project string, now time.Time, staleAfter time.Duration, detail bool) (Worker, []string) {
	readScalar := func(name string) (string, time.Time, error) {
		return readRootScalarWithTime(root, filepath.Join(dir, name))
	}
	readOperational := func(name string) FileMetadata {
		return rootOperationalFile(root, filepath.Join(dir, name))
	}
	worker := Worker{Task: task, Project: project, Health: "orphaned"}
	warnings := []string{}
	var err error
	worker.Status, worker.UpdatedAt, err = readScalar("status")
	if err != nil || !isLifecycle(worker.Status) {
		warnings = append(warnings, fmt.Sprintf("worker %s/%s has invalid status", task, project))
		worker.Status = "unknown"
		worker.Health = "unknown"
	}
	worker.Agent, _, _ = readScalar("agent")
	worker.TDTask, _, _ = readScalar("td_task")
	worker.Worktree, _, _ = readScalar("worktree")
	if detail {
		worker.Branch, _, _ = readScalar("branch")
		worker.Repository, _, _ = readScalar("repository")
		worker.Message = readOperational("message")
		worker.Diagnostic = readOperational("diagnostic")
		worker.Log = readOperational("worker.log")
		worker.Handoff = readOperational("handoff")
	}
	if worker.Worktree != "" {
		if info, statErr := os.Stat(worker.Worktree); statErr == nil && info.IsDir() {
			worker.enrichable = true
		}
	}
	if !detail {
		worker.Worktree = ""
	}
	if worker.Status == "unknown" || worker.Status == "orphaned" {
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

func graphifyMetadata(worktree string) FileMetadata {
	root, err := os.OpenRoot(worktree)
	if err != nil {
		return FileMetadata{Status: "missing"}
	}
	defer root.Close()
	report := rootOperationalFile(root, filepath.Join("graphify-out", "GRAPH_REPORT.md"))
	pending := rootFileMetadata(root, filepath.Join("graphify-out", ".needs_update"))
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

func collectAuditMetadata(fleetRoot *os.Root, workerPath, worktree string) AuditMetadata {
	pending := FileMetadata{}
	if worktree != "" {
		pending = rootedFileMetadata(filepath.Dir(worktree), "response_id")
	}
	// Current Sergeant workers keep transport metadata beside their scalar state,
	// not necessarily in the worktree.
	if !pending.Present {
		pending = rootFileMetadata(fleetRoot, filepath.Join(workerPath, "response_id"))
	}
	acked := rootFileMetadata(fleetRoot, filepath.Join(workerPath, "response_ack"))
	return AuditMetadata{
		ResponsePending:   pending.Present,
		ResponsePendingAt: pending.UpdatedAt,
		ResponseAcked:     acked.Present,
		ResponseAckedAt:   acked.UpdatedAt,
		UpdatedAt:         latestTime(pending.UpdatedAt, acked.UpdatedAt),
	}
}

func rootedFileMetadata(rootPath, path string) FileMetadata {
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return FileMetadata{}
	}
	defer root.Close()
	return rootFileMetadata(root, path)
}

func briefValue(data []byte, key string) (string, bool) {
	prefix := key + ":"
	value := ""
	found := false
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, prefix) {
			if found {
				return "", false
			}
			found = true
			value = strings.TrimSpace(strings.TrimPrefix(line, prefix))
			if key == "Brief" {
				value = compactTitle(value)
			}
		}
	}
	return value, found && value != ""
}

func compactTitle(value string) string {
	if end := strings.Index(value, ". "); end >= 0 {
		value = value[:end]
	}
	const maxTitleBytes = 120
	if len(value) <= maxTitleBytes {
		return value
	}
	value = value[:maxTitleBytes]
	if end := strings.LastIndexByte(value, ' '); end > 0 {
		value = value[:end]
	}
	return strings.TrimSpace(value)
}

func readRootDirBounded(ctx context.Context, root *os.Root, path string, limit int) ([]os.DirEntry, bool, error) {
	if limit <= 0 {
		return []os.DirEntry{}, true, nil
	}
	dir, _, err := openRootDirectory(root, path)
	if err != nil {
		return nil, false, err
	}
	defer dir.Close()
	entries := make([]os.DirEntry, 0, min(limit, 64))
	for {
		if err := ctx.Err(); err != nil {
			return entries, false, err
		}
		batch, readErr := dir.ReadDir(min(64, limit+1-len(entries)))
		entries = append(entries, batch...)
		if len(entries) > limit {
			sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
			return entries[:limit], true, nil
		}
		if readErr == io.EOF {
			sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
			return entries, false, nil
		}
		if readErr != nil {
			return nil, false, readErr
		}
	}
}

func readRootRegularFileBounded(ctx context.Context, root *os.Root, path string, limit int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, info, err := openRootRegular(root, path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if info.Size() > limit {
		return nil, fmt.Errorf("unavailable bounded file")
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, fmt.Errorf("unavailable bounded file")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

func readRootScalarWithTime(root *os.Root, path string) (string, time.Time, error) {
	file, _, err := openRootRegular(root, path)
	if err != nil {
		return "", time.Time{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxScalarBytes+1))
	if err != nil || len(data) > maxScalarBytes {
		return "", time.Time{}, fmt.Errorf("invalid scalar")
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", time.Time{}, fmt.Errorf("invalid scalar")
	}
	return strings.TrimSpace(string(data)), info.ModTime().UTC(), nil
}

func openRootRegular(root *os.Root, path string) (*os.File, os.FileInfo, error) {
	expected, err := rootPathInfo(root, path)
	if err != nil || !expected.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("invalid rooted file")
	}
	file, err := root.Open(path)
	if err != nil {
		return nil, nil, err
	}
	info, err := file.Stat()
	if err != nil || !os.SameFile(expected, info) {
		file.Close()
		return nil, nil, fmt.Errorf("invalid rooted file")
	}
	return file, info, nil
}

func openRootDirectory(root *os.Root, path string) (*os.File, os.FileInfo, error) {
	expected, err := rootPathInfo(root, path)
	if err != nil || !expected.IsDir() {
		return nil, nil, fmt.Errorf("invalid rooted directory")
	}
	directory, err := root.Open(path)
	if err != nil {
		return nil, nil, err
	}
	info, err := directory.Stat()
	if err != nil || !os.SameFile(expected, info) {
		directory.Close()
		return nil, nil, fmt.Errorf("invalid rooted directory")
	}
	return directory, info, nil
}

func rootPathInfo(root *os.Root, path string) (os.FileInfo, error) {
	clean := filepath.Clean(path)
	if filepath.IsAbs(clean) || filepath.VolumeName(clean) != "" {
		return nil, fmt.Errorf("invalid rooted path")
	}
	if clean == "." {
		return root.Lstat(clean)
	}
	current := ""
	parts := strings.Split(clean, string(filepath.Separator))
	for index, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, fmt.Errorf("invalid rooted path")
		}
		current = filepath.Join(current, part)
		info, err := root.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || index < len(parts)-1 && !info.IsDir() {
			return nil, fmt.Errorf("invalid rooted path")
		}
		if index == len(parts)-1 {
			return info, nil
		}
	}
	return nil, fmt.Errorf("invalid rooted path")
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
