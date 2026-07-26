package dashboard

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

const (
	maxProjectedWorkers  = 1000
	maxProjectedWarnings = 100
	maxProjectedChecks   = 200
	maxProjectedComments = 200
)

type projectedState struct {
	CollectedAt     time.Time               `json:"collectedAt"`
	Workers         []projectedSummary      `json:"workers"`
	Warnings        []projectedWarningGroup `json:"warnings"`
	HighestSeverity string                  `json:"highestSeverity,omitempty"`
}

type projectedWarningGroup struct {
	Category  string   `json:"category"`
	Count     int      `json:"count"`
	Label     string   `json:"label"`
	Severity  string   `json:"severity"`
	Items     []string `json:"items,omitempty"`
	Truncated bool     `json:"truncated,omitempty"`
}

type projectedDiagnostics struct {
	Warnings        []projectedWarningGroup `json:"warnings"`
	HighestSeverity string                  `json:"highestSeverity,omitempty"`
}

type projectedSummary struct {
	Task             string    `json:"task"`
	Title            string    `json:"title,omitempty"`
	Project          string    `json:"project"`
	Repository       string    `json:"repository"`
	Status           string    `json:"status"`
	Health           string    `json:"health"`
	Agent            string    `json:"agent,omitempty"`
	TDTask           string    `json:"tdTask,omitempty"`
	PullRequestState string    `json:"pullRequestState,omitempty"`
	DetailKey        string    `json:"detailKey"`
	UpdatedAt        time.Time `json:"updatedAt,omitempty"`
}

type projectedWorker struct {
	Task        string               `json:"task"`
	Title       string               `json:"title,omitempty"`
	Project     string               `json:"project"`
	Repository  string               `json:"repository,omitempty"`
	Status      string               `json:"status"`
	Health      string               `json:"health"`
	Agent       string               `json:"agent,omitempty"`
	Branch      string               `json:"branch,omitempty"`
	TDTask      string               `json:"tdTask,omitempty"`
	Worktree    string               `json:"worktree,omitempty"`
	UpdatedAt   time.Time            `json:"updatedAt,omitempty"`
	Message     projectedFile        `json:"message"`
	Diagnostic  projectedFile        `json:"diagnostic"`
	Log         projectedFile        `json:"log"`
	Handoff     projectedFile        `json:"handoff"`
	TD          projectedFile        `json:"td"`
	Graphify    projectedFile        `json:"graphify"`
	PullRequest projectedPullRequest `json:"pullRequest"`
	NoMistakes  projectedTool        `json:"noMistakes"`
	OCInject    projectedAudit       `json:"ocInject"`
}

type projectedFile struct {
	Present   bool      `json:"present"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
	Summary   string    `json:"summary,omitempty"`
	Status    string    `json:"status,omitempty"`
}

type projectedTool struct {
	Available bool   `json:"available"`
	Phase     string `json:"phase,omitempty"`
	Summary   string `json:"summary,omitempty"`
	Status    string `json:"status,omitempty"`
}

type projectedPullRequest struct {
	URL      string             `json:"url,omitempty"`
	State    string             `json:"state,omitempty"`
	Status   string             `json:"status,omitempty"`
	Checks   []projectedCheck   `json:"checks"`
	Comments []projectedComment `json:"comments"`
}

type projectedCheck struct {
	Name       string `json:"name,omitempty"`
	Context    string `json:"context,omitempty"`
	Status     string `json:"status,omitempty"`
	Conclusion string `json:"conclusion,omitempty"`
	State      string `json:"state,omitempty"`
}
type projectedComment struct {
	Author    string    `json:"author,omitempty"`
	Body      string    `json:"body,omitempty"`
	URL       string    `json:"url,omitempty"`
	CreatedAt time.Time `json:"createdAt,omitempty"`
}
type projectedAudit struct {
	ResponsePending   bool      `json:"responsePending"`
	ResponsePendingAt time.Time `json:"responsePendingAt,omitempty"`
	ResponseAcked     bool      `json:"responseAcked"`
	ResponseAckedAt   time.Time `json:"responseAckedAt,omitempty"`
	UpdatedAt         time.Time `json:"updatedAt,omitempty"`
}

var (
	tdTaskID         = regexp.MustCompile(`^td-[A-Za-z0-9]+$`)
	configIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
	prPath           = regexp.MustCompile(`^/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)/pull/([0-9]+)/?$`)
	githubPath       = regexp.MustCompile(`^[A-Za-z0-9_./-]+$`)
	githubFragment   = regexp.MustCompile(`^(|issuecomment-[0-9]+)$`)
	repositoryName   = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
	credentialURL    = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)[^/@\s]+@`)
	authorization    = regexp.MustCompile(`(?i)(authorization\s*[:=]\s*)[^\r\n]+`)
	assignment       = regexp.MustCompile(`(?i)^([ \t]*(?:export[ \t]+)?([A-Za-z_][A-Za-z0-9_.-]*)[ \t]*[:=][ \t]*)(.*)$`)
	quotedDouble     = regexp.MustCompile(`(?i)((?:api[_-]?key|credential|password|secret|token)\s*[:=]\s*)"[^"]*"`)
	quotedSingle     = regexp.MustCompile(`(?i)((?:api[_-]?key|credential|password|secret|token)\s*[:=]\s*)'[^']*'`)
	basicCredential  = regexp.MustCompile(`(?i)(basic\s+)[A-Za-z0-9+/=]+`)
	privateKeyStart  = regexp.MustCompile(`^-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----$`)
	privateKeyEnd    = regexp.MustCompile(`^-----END (?:[A-Z0-9 ]+ )?PRIVATE KEY-----$`)
	secretValue      = regexp.MustCompile(`(?i)(bearer\s+)[^\s,;]+|((?:api[_-]?key|credential|password|secret|token)\s*[:=]\s*)[^\s,;]+|AKIA[A-Z0-9]{16}|gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,}|npm_[A-Za-z0-9]{20,}|sk-[A-Za-z0-9_-]{20,}|xox[baprs]-[A-Za-z0-9-]{20,}`)
)

func projectState(state State) projectedState {
	workerCount := min(len(state.Workers), maxProjectedWorkers)
	warnings := append([]string(nil), state.Warnings...)
	if len(state.Workers) > workerCount {
		warnings = append(warnings, "projection truncated at safety limit")
	}
	groups, highest := projectWarningGroups(warnings, false)
	projected := projectedState{CollectedAt: state.CollectedAt, Workers: make([]projectedSummary, 0, workerCount), Warnings: groups, HighestSeverity: highest}
	for _, worker := range state.Workers[:workerCount] {
		detailKey := worker.fleetProject
		if detailKey == "" {
			detailKey = worker.Repository
		}
		projected.Workers = append(projected.Workers, projectedSummary{Task: RedactText(worker.Task), Title: RedactText(worker.Title), Project: RedactText(worker.Project), Repository: RedactText(worker.Repository), Status: RedactText(worker.Status), Health: RedactText(worker.Health), Agent: RedactText(worker.Agent), TDTask: validTDTask(worker.TDTask), PullRequestState: RedactText(worker.PullRequest.State), DetailKey: RedactText(detailKey), UpdatedAt: worker.UpdatedAt})
	}
	return projected
}

func projectDiagnostics(warnings []string) projectedDiagnostics {
	groups, highest := projectWarningGroups(warnings, true)
	return projectedDiagnostics{Warnings: groups, HighestSeverity: highest}
}

func projectWarningGroups(warnings []string, includeItems bool) ([]projectedWarningGroup, string) {
	type warningCategory struct {
		category string
		label    string
		severity string
		matches  func(string) bool
	}
	categories := []warningCategory{
		{category: "collection-deadline", label: "collection deadlines", severity: "error", matches: func(value string) bool {
			return strings.Contains(value, "deadline") || strings.Contains(value, "canceled")
		}},
		{category: "fleet-source", label: "unavailable fleet sources", severity: "error", matches: func(value string) bool {
			return value == "fleet source unavailable"
		}},
		{category: "probe-failure", label: "probe failures", severity: "error", matches: func(value string) bool {
			return strings.Contains(value, "probe") && (strings.Contains(value, "failed") || strings.Contains(value, "unavailable"))
		}},
		{category: "invalid-status", label: "invalid fleet statuses", severity: "warning", matches: func(value string) bool { return strings.Contains(value, "invalid status") }},
		{category: "source", label: "missing or corrupt sources", severity: "warning", matches: func(value string) bool {
			return strings.Contains(value, "missing") || strings.Contains(value, "corrupt") || strings.Contains(value, "source unavailable") || strings.Contains(value, "task ") && strings.Contains(value, " unavailable")
		}},
		{category: "truncation", label: "truncated collections", severity: "warning", matches: func(value string) bool { return strings.Contains(value, "truncated") }},
		{category: "other", label: "other fleet diagnostics", severity: "warning", matches: func(string) bool { return true }},
	}
	grouped := make(map[string]*projectedWarningGroup, len(categories))
	remainingItems := maxProjectedWarnings
	for _, raw := range warnings {
		warning := RedactText(raw)
		for _, category := range categories {
			if !category.matches(strings.ToLower(warning)) {
				continue
			}
			group := grouped[category.category]
			if group == nil {
				group = &projectedWarningGroup{Category: category.category, Severity: category.severity}
				grouped[category.category] = group
			}
			group.Count++
			if includeItems {
				if remainingItems > 0 {
					group.Items = append(group.Items, warning)
					remainingItems--
				} else {
					group.Truncated = true
				}
			}
			break
		}
	}
	result := make([]projectedWarningGroup, 0, len(grouped))
	highest := ""
	for _, category := range categories {
		group := grouped[category.category]
		if group == nil {
			continue
		}
		group.Label = fmt.Sprintf("%d %s", group.Count, category.label)
		result = append(result, *group)
		if highest == "" || group.Severity == "error" {
			highest = group.Severity
		}
	}
	return result, highest
}

func projectWorker(worker Worker) projectedWorker {
	return projectedWorker{
		Task: RedactText(worker.Task), Title: RedactText(worker.Title), Project: RedactText(worker.Project), Repository: RedactText(worker.Repository), Status: RedactText(worker.Status), Health: RedactText(worker.Health), Agent: RedactText(worker.Agent), Branch: RedactText(worker.Branch), TDTask: validTDTask(worker.TDTask), Worktree: RedactText(worker.Worktree), UpdatedAt: worker.UpdatedAt,
		Message: projectFile(worker.Message), Diagnostic: projectFile(worker.Diagnostic), Log: projectFile(worker.Log), Handoff: projectFile(worker.Handoff), TD: projectFile(worker.TD), Graphify: projectFile(worker.Graphify), PullRequest: projectPullRequest(worker.PullRequest), NoMistakes: projectTool(worker.NoMistakes),
		OCInject: projectedAudit{ResponsePending: worker.OCInject.ResponsePending, ResponsePendingAt: worker.OCInject.ResponsePendingAt, ResponseAcked: worker.OCInject.ResponseAcked, ResponseAckedAt: worker.OCInject.ResponseAckedAt, UpdatedAt: worker.OCInject.UpdatedAt},
	}
}

func projectFile(file FileMetadata) projectedFile {
	return projectedFile{Present: file.Present, UpdatedAt: file.UpdatedAt, Summary: RedactText(file.Summary), Status: RedactText(file.Status)}
}
func projectTool(tool ToolStatus) projectedTool {
	return projectedTool{Available: tool.Available, Phase: RedactText(tool.Phase), Summary: RedactText(tool.Summary), Status: RedactText(tool.Status)}
}

func projectPullRequest(pr PullRequest) projectedPullRequest {
	checkCount := min(len(pr.Checks), maxProjectedChecks)
	commentCount := min(len(pr.Comments), maxProjectedComments)
	status := RedactText(pr.Status)
	if len(pr.Checks) > checkCount || len(pr.Comments) > commentCount {
		status = strings.TrimSpace(status + " truncated")
	}
	projected := projectedPullRequest{URL: canonicalPullRequestURL(pr.URL), State: RedactText(pr.State), Status: status, Checks: make([]projectedCheck, 0, checkCount), Comments: make([]projectedComment, 0, commentCount)}
	for _, check := range pr.Checks[:checkCount] {
		projected.Checks = append(projected.Checks, projectedCheck{Name: RedactText(check.Name), Context: RedactText(check.Context), Status: RedactText(check.Status), Conclusion: RedactText(check.Conclusion), State: RedactText(check.State)})
	}
	for _, comment := range pr.Comments[:commentCount] {
		projected.Comments = append(projected.Comments, projectedComment{Author: RedactText(comment.Author), Body: RedactText(comment.Body), URL: canonicalGitHubURL(comment.URL), CreatedAt: comment.CreatedAt})
	}
	return projected
}

func validTDTask(value string) string {
	if tdTaskID.MatchString(value) {
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

func canonicalGitHubURL(value string) string {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" || parsed.User != nil {
		return ""
	}
	parsed.RawQuery = ""
	if !githubPath.MatchString(parsed.EscapedPath()) || !githubFragment.MatchString(parsed.Fragment) {
		return ""
	}
	return parsed.String()
}

func repositoryFromRemote(value string) string {
	if strings.HasPrefix(value, "git@github.com:") {
		value = strings.TrimPrefix(value, "git@github.com:")
	} else {
		parsed, err := url.Parse(value)
		if err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" || parsed.User != nil || parsed.RawQuery != "" {
			return ""
		}
		value = strings.TrimPrefix(parsed.Path, "/")
	}
	value = strings.TrimSuffix(value, ".git")
	if repositoryName.MatchString(value) {
		return value
	}
	return ""
}

func RedactText(value string) string {
	if len(value) > maxOperationalBytes {
		return "[content unavailable: oversized]"
	}
	if json.Valid([]byte(value)) {
		var structured any
		if json.Unmarshal([]byte(value), &structured) == nil {
			if data, err := json.Marshal(redactStructured(structured)); err == nil {
				return string(data)
			}
		}
	}
	value = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || r >= 0x20 && r != 0x7f {
			return r
		}
		return -1
	}, value)
	lines := strings.Split(value, "\n")
	redacted := make([]string, 0, len(lines))
	var multilineQuote byte
	inPrivateKey := false
	for _, line := range lines {
		if inPrivateKey {
			if privateKeyEnd.MatchString(strings.TrimSpace(line)) {
				inPrivateKey = false
			}
			continue
		}
		if privateKeyStart.MatchString(strings.TrimSpace(line)) {
			redacted = append(redacted, "[REDACTED PRIVATE KEY]")
			inPrivateKey = true
			continue
		}
		if multilineQuote != 0 {
			if strings.ContainsRune(line, rune(multilineQuote)) {
				multilineQuote = 0
			}
			continue
		}
		if match := assignment.FindStringSubmatch(line); match != nil && isSensitiveKey(match[2], true) {
			rhs := strings.TrimSpace(match[3])
			if len(rhs) > 0 && (rhs[0] == '\'' || rhs[0] == '"') && strings.Count(rhs, string(rhs[0])) == 1 {
				multilineQuote = rhs[0]
			}
			redacted = append(redacted, match[1]+"[REDACTED]")
			continue
		}
		line = credentialURL.ReplaceAllString(line, `${1}[REDACTED]@`)
		line = authorization.ReplaceAllString(line, `${1}[REDACTED]`)
		line = quotedDouble.ReplaceAllString(line, `${1}[REDACTED]`)
		line = quotedSingle.ReplaceAllString(line, `${1}[REDACTED]`)
		line = basicCredential.ReplaceAllString(line, `${1}[REDACTED]`)
		redacted = append(redacted, redactString(line))
	}
	return strings.Join(redacted, "\n")
}

func redactStructured(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		redacted := make(map[string]any, len(typed))
		for key, item := range typed {
			if isSensitiveKey(key, false) {
				redacted[key] = "[REDACTED]"
			} else {
				redacted[key] = redactStructured(item)
			}
		}
		return redacted
	case []any:
		redacted := make([]any, len(typed))
		for i, item := range typed {
			redacted[i] = redactStructured(item)
		}
		return redacted
	case string:
		return RedactText(typed)
	default:
		return value
	}
}

func isSensitiveKey(key string, includeEnvironment bool) bool {
	var normalized strings.Builder
	characters := []rune(key)
	for index, character := range characters {
		isUpper := character >= 'A' && character <= 'Z'
		previousLower := index > 0 && characters[index-1] >= 'a' && characters[index-1] <= 'z'
		nextLower := index+1 < len(characters) && characters[index+1] >= 'a' && characters[index+1] <= 'z'
		if index > 0 && isUpper && (previousLower || nextLower) {
			normalized.WriteByte('_')
		}
		if character == '-' || character == '.' {
			character = '_'
		}
		normalized.WriteRune(character)
	}
	words := strings.FieldsFunc(strings.ToLower(normalized.String()), func(character rune) bool { return character == '_' })
	for _, word := range words {
		switch word {
		case "auth", "authorization", "cookie", "credential", "password", "passwd", "prompt", "secret", "token":
			return true
		case "env", "environment":
			if includeEnvironment {
				return true
			}
		}
	}
	for index := 0; index+1 < len(words); index++ {
		pair := words[index] + "_" + words[index+1]
		switch pair {
		case "access_key", "api_key", "database_url", "injected_message", "initial_message", "private_key", "response_body":
			return true
		}
	}
	return false
}

func rootOperationalFile(root *os.Root, path string) FileMetadata {
	expected, lstatErr := root.Lstat(path)
	if os.IsNotExist(lstatErr) || lstatErr == nil && expected.Mode()&os.ModeSymlink != 0 {
		return FileMetadata{}
	}
	if lstatErr != nil {
		return FileMetadata{Present: true, Summary: "[content unavailable: unreadable]", Status: "unreadable"}
	}
	if !expected.Mode().IsRegular() {
		return FileMetadata{Present: true, UpdatedAt: expected.ModTime().UTC(), Summary: "[content unavailable: corrupt]", Status: "corrupt"}
	}
	file, _, err := openRootRegular(root, path)
	if err != nil {
		return FileMetadata{Present: true, Summary: "[content unavailable: unreadable]", Status: "unreadable"}
	}
	defer file.Close()
	return operationalFileContents(file, expected)
}

func operationalFileContents(file *os.File, expected os.FileInfo) FileMetadata {
	info, err := file.Stat()
	if err != nil || expected != nil && !os.SameFile(expected, info) {
		return FileMetadata{Present: true, Summary: "[content unavailable: unreadable]", Status: "unreadable"}
	}
	if !info.Mode().IsRegular() {
		return FileMetadata{Present: true, UpdatedAt: info.ModTime().UTC(), Summary: "[content unavailable: corrupt]", Status: "corrupt"}
	}
	data, err := io.ReadAll(io.LimitReader(file, maxOperationalBytes+1))
	if err != nil {
		return FileMetadata{Present: true, UpdatedAt: info.ModTime().UTC(), Summary: "[content unavailable: unreadable]", Status: "unreadable"}
	}
	if len(data) > maxOperationalBytes {
		return FileMetadata{Present: true, UpdatedAt: info.ModTime().UTC(), Summary: "[content unavailable: oversized]", Status: "oversized"}
	}
	summary := strings.TrimSpace(RedactText(string(data)))
	if summary == "" {
		return FileMetadata{Present: true, UpdatedAt: info.ModTime().UTC(), Summary: "[content unavailable: corrupt]", Status: "corrupt"}
	}
	return FileMetadata{Present: true, UpdatedAt: info.ModTime().UTC(), Summary: summary, Status: "available"}
}

func redactString(value string) string {
	return secretValue.ReplaceAllStringFunc(value, func(match string) string {
		lower := strings.ToLower(match)
		if strings.HasPrefix(lower, "bearer ") {
			return "Bearer [REDACTED]"
		}
		if index := strings.IndexAny(match, "=:"); index >= 0 {
			end := index + 1
			for end < len(match) && (match[end] == ' ' || match[end] == '\t') {
				end++
			}
			return match[:end] + "[REDACTED]"
		}
		return "[REDACTED]"
	})
}
