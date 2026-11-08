package telegram

import (
	"context"

	"github.com/gotd/td/tg"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
)

// storedInputPeer resolves a chat (marked id) from the stored peers; ok is
// false when it is unknown or has no access hash.
func (l *Listener) storedInputPeer(ctx context.Context, chatID int64) (tg.InputPeerClass, bool, error) {
	self := l.peers.Self()
	if self.ID != 0 && chatID == self.ID {
		return &tg.InputPeerSelf{}, true, nil
	}
	p, ok, err := l.peers.Get(ctx, self.ID, chatID)
	if err != nil {
		return nil, false, wrapf(err, "get peer %d", chatID)
	}
	if !ok {
		return nil, false, nil
	}
	in, ok := inputPeer(p)
	return in, ok, nil
}

// inputPeer builds the input peer of p; ok is false for a user or channel
// without an access hash.
func inputPeer(p PeerInfo) (tg.InputPeerClass, bool) {
	switch p.Kind {
	case jobs.PeerUser:
		if p.IsSelf {
			return &tg.InputPeerSelf{}, true
		}
		return &tg.InputPeerUser{UserID: p.ID, AccessHash: p.AccessHash}, p.AccessHash != 0
	case jobs.PeerChat:
		return &tg.InputPeerChat{ChatID: p.ID}, true
	case jobs.PeerChannel:
		return &tg.InputPeerChannel{ChannelID: p.ID, AccessHash: p.AccessHash}, p.AccessHash != 0
	}
	return nil, false
}
