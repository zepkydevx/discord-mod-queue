// Command bot is the entry point. Run with: go run ./cmd/bot
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bwmarrin/discordgo"

	"github.com/zepkydevx/discord-mod-queue/internal/config"
	"github.com/zepkydevx/discord-mod-queue/internal/discord"
)

func main() {
	settings, err := config.Load()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	session, err := discord.NewSession(settings.DiscordToken)
	if err != nil {
		log.Fatalf("failed to create Discord session: %v", err)
	}

	session.AddHandlerOnce(func(_ *discordgo.Session, r *discordgo.Ready) {
		log.Printf("logged in as %s (ID: %s)", r.User.Username, r.User.ID)
	})

	if err := session.Open(); err != nil {
		log.Fatalf("failed to connect to Discord: %v", err)
	}
	defer session.Close()

	log.Printf("bot is running with %d workers (queue size %d); press Ctrl+C to stop", settings.WorkerCount, settings.QueueSize)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("shutting down")
}
