// Package config loads runtime settings from environment variables, so the
// bot token and tuning knobs never have to live in source code.
package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Settings holds every value the bot needs to start.
type Settings struct {
	DiscordToken string
	WorkerCount  int
	QueueSize    int
	LogLevel     string
}

// Load reads a local .env file if one exists, then builds Settings from the
// environment. Real environment variables always take priority over .env.
func Load() (Settings, error) {
	_ = godotenv.Load() // missing .env is fine; real env vars still work

	token := os.Getenv("DISCORD_TOKEN")
	if token == "" {
		return Settings{}, fmt.Errorf("DISCORD_TOKEN is not set. Copy .env.example to .env and fill it in")
	}

	workerCount, err := envInt("WORKER_COUNT", 4)
	if err != nil {
		return Settings{}, err
	}

	queueSize, err := envInt("QUEUE_SIZE", 100)
	if err != nil {
		return Settings{}, err
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "INFO"
	}

	return Settings{
		DiscordToken: token,
		WorkerCount:  workerCount,
		QueueSize:    queueSize,
		LogLevel:     logLevel,
	}, nil
}

func envInt(name string, fallback int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid integer, got %q", name, raw)
	}
	return value, nil
}
