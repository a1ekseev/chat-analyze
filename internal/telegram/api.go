// Package telegram wraps the gotd client and its PostgreSQL-backed storages.
package telegram

import (
	"context"

	"github.com/gotd/td/tg"
)

//go:generate go tool mockgen -destination=mocks/api.go -package=mocks . API

// API is the subset of the Telegram RPC client used by the service.
type API interface {
	MessagesReadHistory(ctx context.Context, req *tg.MessagesReadHistoryRequest) (*tg.MessagesAffectedMessages, error)
	ChannelsReadHistory(ctx context.Context, req *tg.ChannelsReadHistoryRequest) (bool, error)
	MessagesReadDiscussion(ctx context.Context, req *tg.MessagesReadDiscussionRequest) (bool, error)
	MessagesGetHistory(ctx context.Context, req *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error)
}

var _ API = (*tg.Client)(nil)
