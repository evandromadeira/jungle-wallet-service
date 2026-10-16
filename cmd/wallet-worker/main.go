package main

import (
	"github.com/evandromadeira/jungle-wallet-service/internal/bootstrap"
	"log/slog"
	"os"
)

// main starts local background workers; Fx handles signals and nonzero failure exits.
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	config, err := bootstrap.LoadConfig(os.Getenv)
	if err != nil {
		logger.Error("configuration rejected", "error", err.Error())
		os.Exit(1)
	}
	app := bootstrap.New(config)
	if err := app.Err(); err != nil {
		logger.Error("worker composition failed")
		os.Exit(1)
	}
	app.Run()
}
