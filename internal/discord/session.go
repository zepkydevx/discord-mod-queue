// Package discord wraps discordgo session setup so main stays a thin
// entry point.
package discord

import "github.com/bwmarrin/discordgo"

// NewSession builds a discordgo Session with only the intents this project
// needs. Starting from an explicit, minimal set follows the principle of
// least privilege used across this portfolio's other bots.
func NewSession(token string) (*discordgo.Session, error) {
	session, err := discordgo.New("Bot " + token)
	if err != nil {
		return nil, err
	}

	session.Identify.Intents = discordgo.IntentsGuilds | discordgo.IntentGuildMembers

	return session, nil
}
