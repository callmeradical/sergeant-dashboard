package dashboard

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Source interface {
	Collect(context.Context) State
}

//go:embed web/*
var webAssets embed.FS

func NewHandler(source Source) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", getOnly(func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, map[string]string{"status": "ok"})
	}))
	mux.HandleFunc("/sergeant/api/state", getOnly(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writeJSON(writer, projectState(source.Collect(request.Context())))
	}))
	mux.HandleFunc("/", getOnly(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/" || request.URL.Path == "/sergeant" {
			http.Redirect(writer, request, "/sergeant/", http.StatusTemporaryRedirect)
			return
		}
		http.NotFound(writer, request)
	}))
	assets, err := fs.Sub(webAssets, "web")
	if err != nil {
		panic(err)
	}
	mux.Handle("/sergeant/", getOnly(http.StripPrefix("/sergeant/", http.FileServer(http.FS(assets))).ServeHTTP))
	return securityHeaders(mux)
}

func getOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			writer.Header().Set("Allow", "GET, HEAD")
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		next(writer, request)
	}
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Security-Policy", "default-src 'self'; connect-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; base-uri 'none'; frame-ancestors 'none'")
		writer.Header().Set("Referrer-Policy", "no-referrer")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("X-Frame-Options", "DENY")
		next.ServeHTTP(writer, request)
	})
}

func writeJSON(writer http.ResponseWriter, value any) {
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		http.Error(writer, "encoding response", http.StatusInternalServerError)
	}
}

type projectedState struct {
	CollectedAt time.Time         `json:"collectedAt"`
	Workers     []projectedWorker `json:"workers"`
	Warnings    []string          `json:"warnings"`
}

type projectedWorker struct {
	Task        string                `json:"task"`
	Project     string                `json:"project"`
	Status      string                `json:"status"`
	Health      string                `json:"health"`
	Agent       string                `json:"agent,omitempty"`
	TDTask      string                `json:"tdTask,omitempty"`
	UpdatedAt   time.Time             `json:"updatedAt,omitempty"`
	Message     projectedFileMetadata `json:"message"`
	PullRequest projectedPullRequest  `json:"pullRequest"`
	NoMistakes  ToolStatus            `json:"noMistakes"`
	Graphify    projectedToolMetadata `json:"graphify"`
	OCInject    AuditMetadata         `json:"ocInject"`
}

type projectedFileMetadata struct {
	Present   bool      `json:"present"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
}

type projectedToolMetadata struct {
	Present   bool      `json:"present"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
	Status    string    `json:"status,omitempty"`
}

type projectedPullRequest struct {
	URL    string           `json:"url,omitempty"`
	State  string           `json:"state,omitempty"`
	Checks []projectedCheck `json:"checks"`
}

type projectedCheck struct {
	Name       string `json:"name"`
	Status     string `json:"status,omitempty"`
	Conclusion string `json:"conclusion,omitempty"`
	State      string `json:"state,omitempty"`
}

var (
	tdTaskID = regexp.MustCompile(`^td-[A-Za-z0-9]+$`)
	prPath   = regexp.MustCompile(`^/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)/pull/([0-9]+)/?$`)
	aliasKey = newAliasKey()
)

func projectState(state State) projectedState {
	projected := projectedState{CollectedAt: state.CollectedAt, Workers: make([]projectedWorker, 0, len(state.Workers)), Warnings: make([]string, len(state.Warnings))}
	for index := range projected.Warnings {
		projected.Warnings[index] = "source warning"
	}
	for _, worker := range state.Workers {
		checks := make([]projectedCheck, 0, len(worker.PullRequest.Checks))
		for index, check := range worker.PullRequest.Checks {
			checks = append(checks, projectedCheck{
				Name:       "check " + strconv.Itoa(index+1),
				Status:     allowEnum(strings.ToUpper(check.Status), "", "QUEUED", "IN_PROGRESS", "COMPLETED", "PENDING", "REQUESTED", "WAITING"),
				Conclusion: allowEnum(strings.ToUpper(check.Conclusion), "", "SUCCESS", "FAILURE", "CANCELLED", "SKIPPED", "NEUTRAL", "TIMED_OUT", "ACTION_REQUIRED", "STARTUP_FAILURE", "STALE"),
				State:      allowEnum(strings.ToUpper(check.State), "", "EXPECTED", "ERROR", "FAILURE", "PENDING", "SUCCESS"),
			})
		}
		status := worker.Status
		if strings.HasPrefix(status, "failed:") {
			status = "failed"
		}
		projected.Workers = append(projected.Workers, projectedWorker{
			Task:      opaqueLabel("task", worker.Task),
			Project:   opaqueLabel("project", worker.Project),
			Status:    allowEnum(status, "unknown", "in_progress", "needs_input", "blocked", "done", "failed", "orphaned"),
			Health:    allowEnum(worker.Health, "unknown", "active", "stale", "orphaned", "complete"),
			Agent:     allowEnum(worker.Agent, "", "opencode", "claude", "codex", "gemini"),
			TDTask:    allowMatch(tdTaskID, worker.TDTask),
			UpdatedAt: worker.UpdatedAt,
			Message: projectedFileMetadata{
				Present: worker.Message.Present, UpdatedAt: worker.Message.UpdatedAt,
			},
			PullRequest: projectedPullRequest{
				URL:    canonicalPullRequestURL(worker.PullRequest.URL),
				State:  allowEnum(strings.ToUpper(worker.PullRequest.State), "", "OPEN", "CLOSED", "MERGED"),
				Checks: checks,
			},
			NoMistakes: ToolStatus{
				Available: worker.NoMistakes.Available,
				Phase:     allowEnum(worker.NoMistakes.Phase, "", "intent", "rebase", "review", "test", "document", "lint", "push", "pr", "ci"),
			},
			Graphify: projectedToolMetadata{
				Present: worker.Graphify.Present, UpdatedAt: worker.Graphify.UpdatedAt,
				Status: allowEnum(worker.Graphify.Summary, "", "ready", "update pending"),
			},
			OCInject: worker.OCInject,
		})
	}
	return projected
}

func opaqueLabel(kind, value string) string {
	digest := hmac.New(sha256.New, aliasKey)
	_, _ = digest.Write([]byte(value))
	return fmt.Sprintf("%s-%x", kind, digest.Sum(nil)[:6])
}

func newAliasKey() []byte {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Sprintf("generate alias key: %v", err))
	}
	return key
}

func allowEnum(value, fallback string, allowed ...string) string {
	for _, candidate := range allowed {
		if value == candidate {
			return value
		}
	}
	return fallback
}

func allowMatch(pattern *regexp.Regexp, value string) string {
	if pattern.MatchString(value) {
		return value
	}
	return ""
}

func canonicalPullRequestURL(value string) string {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" || parsed.User != nil {
		return ""
	}
	parts := prPath.FindStringSubmatch(parsed.EscapedPath())
	if parts == nil {
		return ""
	}
	return fmt.Sprintf("https://github.com/%s/%s/pull/%s", parts[1], parts[2], parts[3])
}
