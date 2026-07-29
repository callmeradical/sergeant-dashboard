package dashboard

import (
	"compress/gzip"
	"context"
	"embed"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"sync"
	"time"
)

// stateRequestTimeout is the maximum wall-clock time budget for a single
// /sergeant/api/state, /sergeant/api/diagnostics, or /sergeant/api/workers
// request, covering fleet scan, supervisor probing, and filesystem enrichment.
const stateRequestTimeout = 12 * time.Second

type Source interface {
	Collect(context.Context) State
}

type DetailSource interface {
	CollectDetail(context.Context, string, string) (Worker, bool)
}

type SummarySource interface {
	CollectSummary(context.Context) State
}

type summarySource struct{ Source }

func (source summarySource) Collect(ctx context.Context) State {
	if summaries, ok := source.Source.(SummarySource); ok {
		return summaries.CollectSummary(ctx)
	}
	return source.Source.Collect(ctx)
}

type collectionCall struct {
	done    chan struct{}
	state   State
	cancel  context.CancelFunc
	waiters int
	failed  bool
}

type coalescingSource struct {
	source Source
	mu     sync.Mutex
	active *collectionCall
}

// ttlCache wraps a coalescingSource and serves a cached result for up to ttl.
type ttlCache struct {
	source    *coalescingSource
	ttl       time.Duration
	mu        sync.Mutex
	cached    projectedState
	cachedAt  time.Time
	hasCached bool
}

func (c *ttlCache) get(ctx context.Context) (projectedState, bool) {
	c.mu.Lock()
	if c.hasCached && time.Since(c.cachedAt) < c.ttl {
		result := c.cached
		c.mu.Unlock()
		return result, true
	}
	c.mu.Unlock()

	state, ok := c.source.Collect(ctx)
	if !ok {
		return projectedState{}, false
	}
	projected := projectState(state)

	c.mu.Lock()
	c.cached = projected
	c.cachedAt = time.Now()
	c.hasCached = true
	c.mu.Unlock()

	return projected, true
}

//go:embed web/*
var webAssets embed.FS

func NewHandler(source Source) http.Handler {
	sharedSource := &coalescingSource{source: summarySource{Source: source}}
	cache := &ttlCache{source: sharedSource, ttl: 3 * time.Second}


	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", getOnly(func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, map[string]string{"status": "ok"})
	}))
	mux.HandleFunc("/sergeant/api/state", getOnly(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		ctx, cancel := context.WithTimeout(request.Context(), stateRequestTimeout)
		defer cancel()
		projected, ok := cache.get(ctx)
		if !ok || ctx.Err() != nil {
			writeJSONStatus(writer, http.StatusServiceUnavailable, map[string]string{"error": "fleet state temporarily unavailable"})
			return
		}
		writeJSON(writer, projected)
	}))
	mux.HandleFunc("/sergeant/api/diagnostics", getOnly(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		ctx, cancel := context.WithTimeout(request.Context(), stateRequestTimeout)
		defer cancel()
		state, ok := sharedSource.Collect(ctx)
		if !ok || ctx.Err() != nil {
			writeJSONStatus(writer, http.StatusServiceUnavailable, map[string]string{"error": "fleet diagnostics temporarily unavailable"})
			return
		}
		writeJSON(writer, projectDiagnostics(state.Warnings))
	}))
	mux.HandleFunc("/sergeant/api/workers/", getOnly(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		parts := strings.Split(strings.TrimPrefix(request.URL.Path, "/sergeant/api/workers/"), "/")
		detailSource, supported := source.(DetailSource)
		if !supported || len(parts) != 2 || !configIdentifier.MatchString(parts[0]) || !configIdentifier.MatchString(parts[1]) {
			http.NotFound(writer, request)
			return
		}
		ctx, cancel := context.WithTimeout(request.Context(), stateRequestTimeout)
		defer cancel()
		worker, ok := detailSource.CollectDetail(ctx, parts[0], parts[1])
		if !ok {
			if ctx.Err() != nil {
				writeJSONStatus(writer, http.StatusServiceUnavailable, map[string]string{"error": "worker detail temporarily unavailable"})
				return
			}
			http.NotFound(writer, request)
			return
		}
		writeJSON(writer, projectWorker(worker))
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
	return securityHeaders(gzipMiddleware(mux))
}

// gzipMiddleware compresses responses for clients that accept gzip.
func gzipMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		gz, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}
		defer gz.Close()
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Del("Content-Length")
		next.ServeHTTP(&gzipResponseWriter{ResponseWriter: w, Writer: gz}, r)
	})
}

type gzipResponseWriter struct {
	http.ResponseWriter
	Writer io.Writer
}

func (g *gzipResponseWriter) Write(b []byte) (int, error) {
	return g.Writer.Write(b)
}

func (source *coalescingSource) Collect(ctx context.Context) (State, bool) {
	source.mu.Lock()
	active := source.active
	if active == nil {
		collectionCtx, cancel := context.WithTimeout(context.Background(), stateRequestTimeout)
		active = &collectionCall{done: make(chan struct{}), cancel: cancel}
		source.active = active
		go source.collect(active, collectionCtx)
	}
	active.waiters++
	source.mu.Unlock()

	select {
	case <-active.done:
		return active.state, !active.failed
	case <-ctx.Done():
		source.mu.Lock()
		if source.active == active {
			active.waiters--
			if active.waiters == 0 {
				source.active = nil
				active.cancel()
			}
		}
		source.mu.Unlock()
		return State{}, false
	}
}

func (source *coalescingSource) collect(active *collectionCall, ctx context.Context) {
	state := source.source.Collect(ctx)
	failed := ctx.Err() != nil
	active.cancel()
	source.mu.Lock()
	active.state = state
	active.failed = failed
	if source.active == active {
		source.active = nil
	}
	close(active.done)
	source.mu.Unlock()
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
	writeJSONStatus(writer, http.StatusOK, value)
}

func writeJSONStatus(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		http.Error(writer, "encoding response", http.StatusInternalServerError)
	}
}
