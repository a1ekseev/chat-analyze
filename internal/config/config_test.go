package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testHash = "0123456789abcdef0123456789abcdef"
	testPass = "s3cr3tpassw0rd"
	testDSN  = "postgres://user:" + testPass + "@localhost:5432/db?sslmode=disable"
)

func validEnv() map[string]string {
	return map[string]string{
		"DATABASE_URL": testDSN,
		"TG_API_ID":    "12345",
		"TG_API_HASH":  testHash,
	}
}

func TestLoad_Defaults(t *testing.T) {
	cfg, err := LoadFrom(validEnv())
	require.NoError(t, err)
	require.Equal(t, 1, cfg.Workers)
	require.Equal(t, 10, cfg.JobsMaxAttempts)
	require.Equal(t, 2*time.Minute, cfg.JobsLease)
	require.Equal(t, time.Second, cfg.JobsPollInterval)
	require.Equal(t, 168*time.Hour, cfg.JobsRetention)
	require.Equal(t, 720*time.Hour, cfg.JobsDeadRetention)
	require.True(t, cfg.MarkRead)
	require.Equal(t, "default", cfg.SessionName)
	require.Equal(t, ":8080", cfg.HTTPAddr)
	require.Equal(t, "info", cfg.LogLevel)
	require.Equal(t, "json", cfg.LogFormat)
	require.Equal(t, "warn", cfg.TGLogLevel)
	require.Equal(t, 30*time.Second, cfg.ShutdownTimeout)
	require.Equal(t, testDSN, cfg.DatabaseURL.Reveal())
}

func TestLoad_Invalid(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]string)
		field  string
	}{
		{"missing database url", func(e map[string]string) { delete(e, "DATABASE_URL") }, "DATABASE_URL"},
		{"database url not a url", func(e map[string]string) { e["DATABASE_URL"] = "not a url " + testPass }, "DATABASE_URL"},
		{"database url wrong scheme", func(e map[string]string) { e["DATABASE_URL"] = "http://u:" + testPass + "@h/db" }, "DATABASE_URL"},
		{"api hash short", func(e map[string]string) { e["TG_API_HASH"] = testHash[:31] }, "TG_API_HASH"},
		{"api hash not hex", func(e map[string]string) { e["TG_API_HASH"] = strings.Repeat("z", 32) }, "TG_API_HASH"},
		{"api id zero", func(e map[string]string) { e["TG_API_ID"] = "0" }, "TG_API_ID"},
		{"workers zero", func(e map[string]string) { e["WORKERS"] = "0" }, "WORKERS"},
		{"workers too many", func(e map[string]string) { e["WORKERS"] = "65" }, "WORKERS"},
		{"log level", func(e map[string]string) { e["LOG_LEVEL"] = "verbose" }, "LOG_LEVEL"},
		{"log format", func(e map[string]string) { e["LOG_FORMAT"] = "xml" }, "LOG_FORMAT"},
		{"tg log level", func(e map[string]string) { e["TG_LOG_LEVEL"] = "trace" }, "TG_LOG_LEVEL"},
		{"poll interval too small", func(e map[string]string) { e["JOBS_POLL_INTERVAL"] = "10ms" }, "JOBS_POLL_INTERVAL"},
		{"lease too small", func(e map[string]string) { e["JOBS_LEASE"] = "500ms" }, "JOBS_LEASE"},
		{"shutdown timeout zero", func(e map[string]string) { e["SHUTDOWN_TIMEOUT"] = "0s" }, "SHUTDOWN_TIMEOUT"},
		{"shutdown timeout negative", func(e map[string]string) { e["SHUTDOWN_TIMEOUT"] = "-5s" }, "SHUTDOWN_TIMEOUT"},
		{"shutdown timeout too small", func(e map[string]string) { e["SHUTDOWN_TIMEOUT"] = "500ms" }, "SHUTDOWN_TIMEOUT"},
		{"dead retention zero", func(e map[string]string) { e["JOBS_DEAD_RETENTION"] = "0s" }, "JOBS_DEAD_RETENTION"},
		{"max attempts zero", func(e map[string]string) { e["JOBS_MAX_ATTEMPTS"] = "0" }, "JOBS_MAX_ATTEMPTS"},
		{"unparsable workers", func(e map[string]string) { e["WORKERS"] = "many" + testPass }, "WORKERS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnv()
			tc.mutate(env)
			_, err := LoadFrom(env)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalid)
			require.Contains(t, err.Error(), tc.field)
			for _, secret := range []string{testHash, testHash[:31], testPass} {
				require.NotContains(t, err.Error(), secret)
			}
		})
	}
}

func TestLoad_ReportsAllFields(t *testing.T) {
	env := validEnv()
	env["WORKERS"] = "0"
	env["LOG_LEVEL"] = "verbose"
	_, err := LoadFrom(env)
	require.ErrorIs(t, err, ErrInvalid)
	require.Contains(t, err.Error(), "WORKERS")
	require.Contains(t, err.Error(), "LOG_LEVEL")
	require.NotContains(t, err.Error(), "verbose")
}

func TestSecret_NeverPrinted(t *testing.T) {
	env := validEnv()
	env["TG_PHONE"] = "+79991234567"
	cfg, err := LoadFrom(env)
	require.NoError(t, err)

	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("x", slog.Any("cfg", cfg), slog.Any("hash", cfg.APIHash))
	js, err := json.Marshal(cfg)
	require.NoError(t, err)

	outs := []string{
		fmt.Sprintf("%v %s %+v %#v %v", cfg, cfg.APIHash, cfg, cfg, &cfg),
		fmt.Sprint(cfg.DatabaseURL, cfg.Phone),
		string(js),
		buf.String(),
	}
	for _, o := range outs {
		require.NotContains(t, o, testHash)
		require.NotContains(t, o, testPass)
		require.NotContains(t, o, "79991234567")
		require.Contains(t, o, "[REDACTED]")
	}
	require.Equal(t, `"[REDACTED]"`, string(mustJSON(t, cfg.APIHash)))
	require.Equal(t, testHash, cfg.APIHash.Reveal())
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestValidateLogin(t *testing.T) {
	cfg, err := LoadFrom(validEnv())
	require.NoError(t, err)
	err = cfg.ValidateLogin()
	require.ErrorIs(t, err, ErrInvalid)
	require.Contains(t, err.Error(), "TG_PHONE")

	for _, bad := range []string{"+7 999", "abc", "+0123456789"} {
		env := validEnv()
		env["TG_PHONE"] = bad
		cfg, err := LoadFrom(env)
		require.NoError(t, err)
		err = cfg.ValidateLogin()
		require.ErrorIs(t, err, ErrInvalid)
		require.NotContains(t, err.Error(), bad)
	}

	env := validEnv()
	env["TG_PHONE"] = "+79991234567"
	cfg, err = LoadFrom(env)
	require.NoError(t, err)
	require.NoError(t, cfg.ValidateLogin())
}

func TestLoadDatabaseFrom(t *testing.T) {
	dsn, err := LoadDatabaseFrom(map[string]string{"DATABASE_URL": testDSN})
	require.NoError(t, err, "Telegram credentials are not needed")
	require.Equal(t, testDSN, dsn.Reveal())

	for name, env := range map[string]map[string]string{
		"missing": {},
		"empty":   {"DATABASE_URL": ""},
		"bad":     {"DATABASE_URL": "mysql://u:" + testPass + "@h/db"},
	} {
		_, err := LoadDatabaseFrom(env)
		require.ErrorIs(t, err, ErrInvalid, name)
		require.Contains(t, err.Error(), "DATABASE_URL", name)
		require.NotContains(t, err.Error(), testPass, name)
	}
}

func TestLoad_ShutdownTimeoutMinimum(t *testing.T) {
	env := validEnv()
	env["SHUTDOWN_TIMEOUT"] = "1s"
	cfg, err := LoadFrom(env)
	require.NoError(t, err)
	require.Equal(t, time.Second, cfg.ShutdownTimeout)
}

func TestLoad_DeadRetention(t *testing.T) {
	env := validEnv()
	env["JOBS_DEAD_RETENTION"] = "48h"
	cfg, err := LoadFrom(env)
	require.NoError(t, err)
	require.Equal(t, 48*time.Hour, cfg.JobsDeadRetention)
	require.Contains(t, cfg.String(), "jobs_dead_retention=48h0m0s")
}
