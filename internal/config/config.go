// Package config loads and validates process configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/go-playground/validator/v10"
)

// ErrInvalid is wrapped by every configuration error. Messages list only
// environment variable names, never values.
var ErrInvalid = errors.New("invalid config")

const (
	minJobsLease        = time.Second
	minJobsPollInterval = 100 * time.Millisecond
	minShutdownTimeout  = time.Second
)

// Config is the process configuration (spec §9).
type Config struct {
	DatabaseURL Secret `env:"DATABASE_URL,required" validate:"required,url"`

	APIID       int    `env:"TG_API_ID,required" validate:"required,gt=0"`
	APIHash     Secret `env:"TG_API_HASH,required" validate:"required,len=32,hexadecimal"`
	SessionName string `env:"TG_SESSION_NAME" envDefault:"default" validate:"required"`
	Phone       Secret `env:"TG_PHONE"`

	Workers          int           `env:"WORKERS" envDefault:"1" validate:"min=1,max=64"`
	JobsMaxAttempts  int           `env:"JOBS_MAX_ATTEMPTS" envDefault:"10" validate:"min=1"`
	JobsLease        time.Duration `env:"JOBS_LEASE" envDefault:"2m"`
	JobsPollInterval time.Duration `env:"JOBS_POLL_INTERVAL" envDefault:"1s"`
	JobsRetention    time.Duration `env:"JOBS_RETENTION" envDefault:"168h"`
	// JobsDeadRetention is how long dead jobs (whose payload holds message
	// text) are kept for inspection before maintenance deletes them.
	JobsDeadRetention time.Duration `env:"JOBS_DEAD_RETENTION" envDefault:"720h"`

	MarkRead bool   `env:"MARK_READ" envDefault:"true"`
	HTTPAddr string `env:"HTTP_ADDR" envDefault:":8080"`

	LogLevel        string        `env:"LOG_LEVEL" envDefault:"info" validate:"oneof=debug info warn error"`
	LogFormat       string        `env:"LOG_FORMAT" envDefault:"json" validate:"oneof=json text"`
	TGLogLevel      string        `env:"TG_LOG_LEVEL" envDefault:"warn" validate:"oneof=debug info warn error"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"30s"`
}

// Load reads configuration from the process environment.
func Load() (Config, error) { return LoadFrom(nil) }

// LoadFrom reads configuration from environ; nil means the process environment.
func LoadFrom(environ map[string]string) (Config, error) {
	cfg, err := env.ParseAsWithOptions[Config](env.Options{Environment: environ})
	if err != nil {
		return Config{}, invalid(parseFields(err))
	}
	var bad []string
	if err := validator.New(validator.WithRequiredStructEnabled()).Struct(cfg); err != nil {
		if verrs, ok := errors.AsType[validator.ValidationErrors](err); ok {
			for _, fe := range verrs {
				bad = append(bad, envName(fe.StructField()))
			}
		} else {
			bad = append(bad, "config")
		}
	}
	if cfg.JobsLease < minJobsLease {
		bad = append(bad, "JOBS_LEASE")
	}
	if cfg.JobsPollInterval < minJobsPollInterval {
		bad = append(bad, "JOBS_POLL_INTERVAL")
	}
	if cfg.JobsDeadRetention <= 0 {
		bad = append(bad, "JOBS_DEAD_RETENTION")
	}
	// A zero or tiny timeout would make every shutdown "timed out" (exit 1).
	if cfg.ShutdownTimeout < minShutdownTimeout {
		bad = append(bad, "SHUTDOWN_TIMEOUT")
	}
	if !validDatabaseURL(cfg.DatabaseURL.Reveal()) {
		bad = append(bad, "DATABASE_URL")
	}
	if len(bad) > 0 {
		return Config{}, invalid(bad)
	}
	return cfg, nil
}

// ValidateLogin checks the extra requirements of the interactive login command.
func (c Config) ValidateLogin() error {
	if err := validator.New().Var(string(c.Phone), "required,e164"); err != nil {
		return invalid([]string{"TG_PHONE"})
	}
	return nil
}

// LogValue implements slog.LogValuer; secrets are redacted by their own type.
func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("database_url", c.DatabaseURL.String()),
		slog.Int("tg_api_id", c.APIID),
		slog.String("tg_api_hash", c.APIHash.String()),
		slog.String("tg_session_name", c.SessionName),
		slog.String("tg_phone", c.Phone.String()),
		slog.Int("workers", c.Workers),
		slog.Int("jobs_max_attempts", c.JobsMaxAttempts),
		slog.Duration("jobs_lease", c.JobsLease),
		slog.Duration("jobs_poll_interval", c.JobsPollInterval),
		slog.Duration("jobs_retention", c.JobsRetention),
		slog.Duration("jobs_dead_retention", c.JobsDeadRetention),
		slog.Bool("mark_read", c.MarkRead),
		slog.String("http_addr", c.HTTPAddr),
		slog.String("log_level", c.LogLevel),
		slog.String("log_format", c.LogFormat),
		slog.String("tg_log_level", c.TGLogLevel),
		slog.Duration("shutdown_timeout", c.ShutdownTimeout),
	)
}

// String implements fmt.Stringer; it renders the same safe view as LogValue.
func (c Config) String() string { return c.LogValue().String() }

// GoString implements fmt.GoStringer (covers %#v).
func (c Config) GoString() string { return c.String() }

func invalid(fields []string) error {
	sort.Strings(fields)
	fields = compact(fields)
	return fmt.Errorf("%w: %s", ErrInvalid, strings.Join(fields, ", "))
}

func compact(s []string) []string {
	out := s[:0]
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}

// validDatabaseURL accepts postgres/postgresql URLs with a non-empty host.
func validDatabaseURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return (u.Scheme == "postgres" || u.Scheme == "postgresql") && u.Hostname() != ""
}

// parseFields extracts env variable names from an env library error without
// including its text, which may echo the offending value.
func parseFields(err error) []string {
	var names []string
	var walk func(error)
	walk = func(e error) {
		if agg, ok := errors.AsType[env.AggregateError](e); ok {
			for _, inner := range agg.Errors {
				walk(inner)
			}
			return
		}
		if pe, ok := errors.AsType[env.ParseError](e); ok {
			names = append(names, envName(pe.Name))
		} else if ns, ok := errors.AsType[env.VarIsNotSetError](e); ok {
			names = append(names, ns.Key)
		} else if ev, ok := errors.AsType[env.EmptyVarError](e); ok {
			names = append(names, ev.Key)
		} else {
			names = append(names, "config")
		}
	}
	walk(err)
	return names
}

// envName maps a Config struct field name to its environment variable name.
func envName(field string) string {
	sf, ok := reflect.TypeFor[Config]().FieldByName(field)
	if !ok {
		return "config"
	}
	name, _, _ := strings.Cut(sf.Tag.Get("env"), ",")
	if name == "" {
		return "config"
	}
	return name
}

// LoadDatabaseFrom reads and validates only DATABASE_URL (for commands such
// as migrate that need nothing else); nil environ means the process env.
// Errors name the variable, never its value.
func LoadDatabaseFrom(environ map[string]string) (Secret, error) {
	type dbOnly struct {
		DatabaseURL Secret `env:"DATABASE_URL"`
	}
	c, err := env.ParseAsWithOptions[dbOnly](env.Options{Environment: environ})
	if err != nil || !validDatabaseURL(c.DatabaseURL.Reveal()) {
		return "", invalid([]string{"DATABASE_URL"})
	}
	return c.DatabaseURL, nil
}
