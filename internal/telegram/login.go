package telegram

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
	"golang.org/x/term"

	"github.com/a1ekseev/chat-analyze/internal/logx"
)

// Login interactively authorizes the account: it sends a login code to
// phone, reads the code (and the 2FA password when Telegram asks for it)
// from in, and stores the session in PostgreSQL. When in is a terminal the
// input is read without echo. The phone is only ever printed masked.
func Login(ctx context.Context, cfg ClientConfig, phone string, in io.Reader, out io.Writer) error {
	client := telegram.NewClient(cfg.APIID, cfg.APIHash, telegram.Options{
		SessionStorage: NewSessionStorage(cfg.Pool, cfg.SessionName),
		Logger:         logx.NewGotdLogger(cfg.Logger, cfg.GotdLogLevel),
		NoUpdates:      true,
	})
	p := newPrompter(in, out)

	err := client.Run(ctx, func(ctx context.Context) error {
		status, err := client.Auth().Status(ctx)
		if err != nil {
			return fmt.Errorf("auth status: %w", err)
		}
		if status.Authorized {
			_, _ = fmt.Fprintln(out, "Already logged in, session is stored.")
			return nil
		}
		_, _ = fmt.Fprintf(out, "Sending login code to %s\n", MaskPhone(phone))
		flow := auth.NewFlow(userAuth{phone: phone, p: p}, auth.SendCodeOptions{})
		if err := client.Auth().IfNecessary(ctx, flow); err != nil {
			return fmt.Errorf("auth flow: %w", err)
		}
		_, _ = fmt.Fprintln(out, "Logged in, session saved.")
		return nil
	})
	if err != nil {
		return fmt.Errorf("login: %w", err)
	}
	return nil
}

// MaskPhone keeps the leading "+", the first digit and the last four
// characters: "+79991234567" → "+7******4567". Short inputs are fully masked.
func MaskPhone(phone string) string {
	r := []rune(phone)
	prefix := 0
	if len(r) > 0 && r[0] == '+' {
		prefix = 1
	}
	const keepTail = 4
	if len(r)-prefix < keepTail+3 {
		return string(r[:prefix]) + strings.Repeat("*", len(r)-prefix)
	}
	head := prefix + 1
	return string(r[:head]) + strings.Repeat("*", len(r)-head-keepTail) + string(r[len(r)-keepTail:])
}

// userAuth answers gotd's auth flow; the password is only asked for when the
// account has 2FA enabled.
type userAuth struct {
	phone string
	p     *prompter
}

func (a userAuth) Phone(context.Context) (string, error) { return a.phone, nil }

func (a userAuth) Password(context.Context) (string, error) {
	// The password is used verbatim: spaces may be part of it.
	return a.p.ask("Enter 2FA password: ")
}

func (a userAuth) Code(context.Context, *tg.AuthSentCode) (string, error) {
	code, err := a.p.ask("Enter login code: ")
	return strings.TrimSpace(code), err
}

func (userAuth) AcceptTermsOfService(_ context.Context, tos tg.HelpTermsOfService) error {
	return &auth.SignUpRequired{TermsOfService: tos}
}

func (userAuth) SignUp(context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, errors.New("sign up is not supported, register the account in an official app")
}

// prompter reads answers without echo from a terminal, line by line otherwise.
type prompter struct {
	in     *bufio.Reader
	out    io.Writer
	fd     int
	isTerm bool
}

func newPrompter(in io.Reader, out io.Writer) *prompter {
	p := &prompter{in: bufio.NewReader(in), out: out}
	if f, ok := in.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		p.fd, p.isTerm = int(f.Fd()), true
	}
	return p
}

func (p *prompter) ask(prompt string) (string, error) {
	_, _ = fmt.Fprint(p.out, prompt)
	if p.isTerm {
		b, err := term.ReadPassword(p.fd)
		_, _ = fmt.Fprintln(p.out)
		if err != nil {
			return "", fmt.Errorf("read input: %w", err)
		}
		return string(b), nil
	}
	line, err := p.in.ReadString('\n')
	if err != nil && (line == "" || !errors.Is(err, io.EOF)) {
		return "", fmt.Errorf("read input: %w", err)
	}
	return strings.TrimRight(line, "\r\n"), nil
}
