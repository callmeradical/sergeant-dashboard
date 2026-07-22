package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
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
		writeJSON(writer, source.Collect(request.Context()))
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
		writer.Header().Set("Content-Security-Policy", "default-src 'self'; connect-src 'self'; img-src 'self'; style-src 'self'; script-src 'self'; base-uri 'none'; frame-ancestors 'none'")
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
