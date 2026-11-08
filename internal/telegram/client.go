package telegram

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/telegram/updates/hook"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/a1ekseev/chat-analyze/internal/logx"
)

// ErrNoSession is returned by Client.Run when no authorized session is stored.
var ErrNoSession = errors.New("telegram session not found, run \"chat-analyze login\"")

// ClientConfig configures the gotd client.
type ClientConfig struct {
	APIID        int
	APIHash      string
	Pool         *pgxpool.Pool
	SessionName  string
	Logger       *slog.Logger
	GotdLogLevel slog.Level
	// RPS is the account-wide RPC rate (RateLimiter); 0 means 10.
	RPS float64
	// Peers receives the current account once authorized (optional).
	Peers *PeerCache
	// OnChannelTooLong is called when updates.Manager cannot recover a
	// channel gap (optional).
	OnChannelTooLong func(channelID int64)
}

// Client wires the gotd client, the peers manager and updates.Manager
// (gap recovery with state in PostgreSQL) to an update dispatcher.
type Client struct {
	raw        *telegram.Client
	gaps       *updates.Manager
	pmgr       *peers.Manager
	session    *SessionStorage
	dispatcher *tg.UpdateDispatcher
	peers      *PeerCache
	log        *slog.Logger
	ready      atomic.Bool
}

// NewClient builds the client. Nothing connects until Run.
//
// Update chain: telegram.Client → peers.UpdateHook (access hashes) →
// updates.Manager (gaps, pts in PG) → peers.UpdateHook → dispatcher.
func NewClient(cfg ClientConfig) *Client {
	log := cfg.Logger.With(logx.Component("telegram"))
	gotdLogger := logx.NewGotdLogger(cfg.Logger, cfg.GotdLogLevel)
	c := &Client{peers: cfg.Peers, log: log}

	d := tg.NewUpdateDispatcher()
	c.dispatcher = &d
	state := NewStateStorage(cfg.Pool)

	// The peers manager needs the raw client while both ends of the update
	// chain need the peers manager: bind the handlers after construction.
	inner, outer := &lateHandler{}, &lateHandler{}
	c.gaps = updates.New(updates.Config{
		Handler:          inner,
		Storage:          state,
		AccessHasher:     state,
		UserAccessHasher: state,
		OnChannelTooLong: func(channelID int64) {
			if cfg.OnChannelTooLong != nil {
				cfg.OnChannelTooLong(channelID)
			}
		},
		OnChannelInaccessible: func(channelID int64) {
			log.WarnContext(context.Background(), "channel inaccessible", logx.ChatID(channelChatID(channelID)))
		},
		OnTooLong: func() {
			log.WarnContext(context.Background(), "updates too long")
		},
		OnLoadUserStateFailed: func() {
			log.WarnContext(context.Background(), "load user state failed")
		},
		OnLoadChannelStateFailed: func(channelID int64) {
			log.WarnContext(context.Background(), "load channel state failed", logx.ChatID(channelChatID(channelID)))
		},
		Logger: gotdLogger,
		// Bounds the getChannelDifference burst at startup and reconnect
		// (one per stored channel); 3 keeps gap recovery reasonably fast
		// while leaving RateLimiter slots for the workers.
		MaxChannelDifferenceConcurrency: 3,
	})

	c.session = NewSessionStorage(cfg.Pool, cfg.SessionName)
	c.raw = telegram.NewClient(cfg.APIID, cfg.APIHash, telegram.Options{
		SessionStorage: c.session,
		UpdateHandler:  outer,
		Middlewares: []telegram.Middleware{
			NewRateLimiter(cmp.Or(cfg.RPS, 10), cfg.Logger),
			hook.AffectedHook(c.gaps),
		},
		Logger: gotdLogger,
	})
	c.pmgr = peers.Options{Storage: NewPeerKV(cfg.Pool), Logger: gotdLogger}.Build(c.raw.API())
	inner.h = c.pmgr.UpdateHook(c.dispatcher)
	outer.h = c.pmgr.UpdateHook(c.gaps)
	return c
}

// lateHandler forwards to a handler assigned after construction.
type lateHandler struct{ h telegram.UpdateHandler }

func (l *lateHandler) Handle(ctx context.Context, u tg.UpdatesClass) error {
	return l.h.Handle(ctx, u) //nolint:wrapcheck // transparent forwarder
}

// API returns the RPC client; valid right after NewClient, usable once Run
// has connected.
func (c *Client) API() API { return c.raw.API() }

// Ready reports whether the client is connected and authorized.
func (c *Client) Ready() bool { return c.ready.Load() }

// Run registers handlers via register, connects, checks authorization and
// processes updates until ctx is cancelled. It returns ErrNoSession when no
// authorized session is stored; when no session is stored at all it does so
// before connecting (no network, no new auth key, nothing written).
func (c *Client) Run(ctx context.Context, register func(d *tg.UpdateDispatcher)) error {
	if _, err := c.session.LoadSession(ctx); err != nil {
		if errors.Is(err, session.ErrNotFound) {
			c.log.ErrorContext(ctx, "telegram auth required", logx.Err(ErrNoSession))
			return ErrNoSession
		}
		return fmt.Errorf("check telegram session: %w", err)
	}
	if register != nil {
		register(c.dispatcher)
	}
	defer c.ready.Store(false)

	err := c.raw.Run(ctx, func(ctx context.Context) error {
		status, err := c.raw.Auth().Status(ctx)
		if err != nil {
			return fmt.Errorf("auth status: %w", err)
		}
		if !status.Authorized {
			c.log.ErrorContext(ctx, "telegram auth required", logx.Err(ErrNoSession))
			return ErrNoSession
		}
		self, err := c.raw.Self(ctx)
		if err != nil {
			return fmt.Errorf("get self: %w", err)
		}
		if err := c.pmgr.Init(ctx); err != nil {
			return fmt.Errorf("init peers: %w", err)
		}
		if c.peers != nil {
			c.peers.SetSelf(self)
		}
		c.ready.Store(true)
		c.log.InfoContext(ctx, "telegram connected",
			slog.Int64("self_id", self.ID), slog.Int("dc", c.raw.Config().ThisDC))

		if err := c.gaps.Run(ctx, c.raw.API(), self.ID, updates.AuthOptions{IsBot: false}); err != nil {
			return fmt.Errorf("updates manager: %w", err)
		}
		return nil
	})
	if err != nil && ctx.Err() == nil && !errors.Is(err, ErrNoSession) {
		c.log.WarnContext(ctx, "telegram disconnected", logx.Err(err))
	}
	if ctx.Err() != nil {
		return nil // cancellation is the normal way to stop
	}
	if err != nil {
		return fmt.Errorf("telegram client: %w", err)
	}
	return nil
}
