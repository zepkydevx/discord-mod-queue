package discord

import (
	"context"
	"fmt"

	"github.com/bwmarrin/discordgo"
)

// RoleToggleJob adds a role to a member and immediately removes it again —
// a harmless, repeatable action against the real Discord API, used to
// demonstrate the queue without banning or kicking anyone.
type RoleToggleJob struct {
	Session *discordgo.Session
	GuildID string
	UserID  string
	RoleID  string
}

func (j *RoleToggleJob) Describe() string {
	return fmt.Sprintf("toggle role %s on user %s", j.RoleID, j.UserID)
}

// Execute runs one attempt. Note: discordgo's REST helpers below don't take
// a context.Context mid-request, so cancellation here only takes effect
// before the call starts; the queue's rate limiter is what actually
// enforces ctx cancellation between attempts.
func (j *RoleToggleJob) Execute(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := j.Session.GuildMemberRoleAdd(j.GuildID, j.UserID, j.RoleID); err != nil {
		return fmt.Errorf("add role: %w", err)
	}
	if err := j.Session.GuildMemberRoleRemove(j.GuildID, j.UserID, j.RoleID); err != nil {
		return fmt.Errorf("remove role: %w", err)
	}
	return nil
}
