package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/SeaRoll/mqs"
)

type config struct {
	databaseURL string
	listenPool  int
	port        int
}

func mustNewConfig() config {
	if os.Getenv("DATABASE_URL") == "" {
		panic("DATABASE_URL environment variable is not set")
	}

	port := 8080
	if p := os.Getenv("PORT"); p != "" {
		if parsedPort, err := strconv.Atoi(p); err == nil {
			port = parsedPort
		}
	}

	listenPool := 10
	if p := os.Getenv("LISTEN_POOL"); p != "" {
		if parsedPool, err := strconv.Atoi(p); err == nil {
			listenPool = parsedPool
		}
	}

	return config{
		databaseURL: os.Getenv("DATABASE_URL"),
		port:        port,
		listenPool:  listenPool,
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg := mustNewConfig()

	app, err := mqs.InitApp(ctx, cfg.databaseURL)
	if err != nil {
		panic(err)
	}
	defer app.Close()

	var publishWg sync.WaitGroup

	slog.Info("Starting MQS server", "port", cfg.port, "listenPool", cfg.listenPool)

	for range cfg.listenPool {
		publishWg.Add(1)
		go func() {
			defer publishWg.Done()
			app.PublishMesssages(ctx)
		}()
	}

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- app.RunServer(ctx, cfg.port)
	}()

	select {
	case err := <-serverDone:
		if err != nil {
			slog.Error("Server error", "error", err)
		}
	case <-ctx.Done():
		slog.Info("Shutdown signal received, waiting for message publishers to finish...")
	}

	cancel()

	publishWg.Wait()
	slog.Info("All message publishing goroutines have stopped")
}
