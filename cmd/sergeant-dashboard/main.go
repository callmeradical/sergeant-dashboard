package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/callmeradical/sergeant-dashboard/internal/dashboard"
)

func main() {
	if len(os.Args) > 1 {
		if len(os.Args) != 4 || os.Args[1] != "validate-serve" {
			fmt.Fprintln(os.Stderr, "usage: sergeant-dashboard validate-serve HTTPS_URL BACKEND")
			os.Exit(2)
		}
		if err := dashboard.ValidateServeURL(os.Stdin, os.Args[2], os.Args[3]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	config := dashboard.ConfigFromEnv(os.Getenv)
	collector := dashboard.Collector{FleetRoot: config.FleetRoot, StaleAfter: config.StaleAfter, Limit: config.Limit}
	server := &http.Server{
		Addr:              config.Address,
		Handler:           dashboard.NewHandler(collector),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()

	log.Printf("sergeant dashboard listening on http://%s/sergeant/", config.Address)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
