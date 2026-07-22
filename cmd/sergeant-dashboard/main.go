package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/callmeradical/sergeant-dashboard/internal/dashboard"
)

func main() {
	config := dashboard.ConfigFromEnv(os.Getenv)
	collector := dashboard.Collector{FleetRoot: config.FleetRoot, StaleAfter: config.StaleAfter}
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
