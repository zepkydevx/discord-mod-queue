// Command bot is the entry point. Run with: go run ./cmd/bot
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bwmarrin/discordgo"

	"github.com/zepkydevx/discord-mod-queue/internal/config"
	"github.com/zepkydevx/discord-mod-queue/internal/discord"
	"github.com/zepkydevx/discord-mod-queue/internal/queue"
	"github.com/zepkydevx/discord-mod-queue/internal/ratelimit"
)

var demoCommand = &discordgo.ApplicationCommand{
	Name:        "queue-demo",
	Description: "Enqueue a burst of role-toggle jobs to demonstrate the moderation queue",
	Options: []*discordgo.ApplicationCommandOption{
		{
			Type:        discordgo.ApplicationCommandOptionInteger,
			Name:        "count",
			Description: "How many jobs to enqueue (1-20)",
			Required:    true,
		},
		{
			Type:        discordgo.ApplicationCommandOptionRole,
			Name:        "role",
			Description: "A harmless role to add and remove repeatedly",
			Required:    true,
		},
	},
}

func main() {
	settings, err := config.Load()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	session, err := discord.NewSession(settings.DiscordToken)
	if err != nil {
		log.Fatalf("failed to create Discord session: %v", err)
	}

	limiter := ratelimit.New(5, 1) // burst of 5, refilling 1 token/second
	jobQueue := queue.New(settings.WorkerCount, settings.QueueSize, limiter, queue.DefaultRetryPolicy)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobQueue.Start(ctx)

	session.AddHandlerOnce(func(_ *discordgo.Session, r *discordgo.Ready) {
		log.Printf("logged in as %s (ID: %s)", r.User.Username, r.User.ID)
	})

	// Registering the command per-guild (rather than globally) makes it
	// available instantly in whatever test server the bot joins, instead
	// of waiting for Discord's slower global command propagation.
	session.AddHandler(func(s *discordgo.Session, g *discordgo.GuildCreate) {
		if _, err := s.ApplicationCommandCreate(s.State.User.ID, g.ID, demoCommand); err != nil {
			log.Printf("failed to register /queue-demo in guild %s: %v", g.ID, err)
			return
		}
		log.Printf("registered /queue-demo in guild %s", g.ID)
	})

	session.AddHandler(handleQueueDemo(jobQueue))

	if err := session.Open(); err != nil {
		log.Fatalf("failed to connect to Discord: %v", err)
	}
	defer session.Close()

	log.Printf("bot is running with %d workers (queue size %d); press Ctrl+C to stop", settings.WorkerCount, settings.QueueSize)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("shutting down")
	cancel()
	jobQueue.Stop()
}

func handleQueueDemo(jobQueue *queue.Queue) func(s *discordgo.Session, i *discordgo.InteractionCreate) {
	return func(s *discordgo.Session, i *discordgo.InteractionCreate) {
		if i.Type != discordgo.InteractionApplicationCommand {
			return
		}
		data := i.ApplicationCommandData()
		if data.Name != "queue-demo" {
			return
		}

		count := int(data.Options[0].IntValue())
		if count < 1 {
			count = 1
		}
		if count > 20 {
			count = 20
		}
		roleID := data.Options[1].RoleValue(s, i.GuildID).ID
		userID := i.Member.User.ID

		enqueued := 0
		for n := 0; n < count; n++ {
			job := &discord.RoleToggleJob{
				Session: s,
				GuildID: i.GuildID,
				UserID:  userID,
				RoleID:  roleID,
			}
			if err := jobQueue.Enqueue(job); err != nil {
				break
			}
			enqueued++
		}

		content := fmt.Sprintf("Queued %d/%d jobs — watch the console for progress.", enqueued, count)
		_ = s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: content,
				Flags:   discordgo.MessageFlagsEphemeral,
			},
		})
	}
}
