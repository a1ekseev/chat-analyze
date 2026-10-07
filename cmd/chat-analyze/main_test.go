package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRun_UnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run(context.Background(), []string{"nope"}, &stdout, &stderr)

	assert.Equal(t, 2, code)
	assert.Contains(t, stderr.String(), "usage: chat-analyze run|login|migrate|healthcheck|version")
}

func TestRun_Version(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run(context.Background(), []string{"version"}, &stdout, &stderr)

	assert.Equal(t, 0, code)
	assert.Equal(t, version+" "+commit+"\n", stdout.String())
}

const secretHash = "0123456789abcdef0123456789abcdef"

func setInvalidEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "")
	require.NoError(t, os.Unsetenv("DATABASE_URL"))
	t.Setenv("TG_API_ID", "12345")
	t.Setenv("TG_API_HASH", secretHash)
	t.Setenv("TG_PHONE", "+79991234567")
}

func TestRun_MigrateRequiresDB(t *testing.T) {
	setInvalidEnv(t)
	var stdout, stderr bytes.Buffer

	code := run(context.Background(), []string{"migrate", "up"}, &stdout, &stderr)

	assert.Equal(t, 1, code)
	assert.Contains(t, stderr.String(), "DATABASE_URL")
	assert.NotContains(t, stderr.String(), secretHash)
	assert.NotContains(t, stderr.String(), "79991234567")
	assert.Empty(t, stdout.String())
}

func TestRun_MigrateUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"migrate", "sideways"}, &stdout, &stderr)
	assert.Equal(t, 2, code)
	assert.Contains(t, stderr.String(), "migrate [up|down|status]")
}

func TestRun_RunRequiresConfig(t *testing.T) {
	setInvalidEnv(t)
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"run"}, &stdout, &stderr)
	assert.Equal(t, 1, code)
	assert.Contains(t, stderr.String(), "DATABASE_URL")
	assert.NotContains(t, stderr.String(), secretHash)
}

func TestRun_LoginRequiresPhone(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u:CANARY-pw@127.0.0.1:1/db")
	t.Setenv("TG_API_ID", "12345")
	t.Setenv("TG_API_HASH", secretHash)
	t.Setenv("TG_PHONE", "not-a-phone")
	var stdout, stderr bytes.Buffer

	code := run(context.Background(), []string{"login"}, &stdout, &stderr)

	assert.Equal(t, 1, code)
	assert.Contains(t, stderr.String(), "TG_PHONE")
	assert.NotContains(t, stderr.String(), "CANARY")
	assert.NotContains(t, stderr.String(), "not-a-phone")
	assert.NotContains(t, stderr.String(), secretHash)
}

func TestRun_Healthcheck(t *testing.T) {
	setInvalidEnv(t) // healthcheck must not need the full config
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	t.Setenv("HTTP_ADDR", ":"+u.Port())

	var stdout, stderr bytes.Buffer
	assert.Equal(t, 0, run(context.Background(), []string{"healthcheck"}, &stdout, &stderr))
	status = http.StatusServiceUnavailable
	assert.Equal(t, 1, run(context.Background(), []string{"healthcheck"}, &stdout, &stderr))
}

func TestRun_MigrateNeedsOnlyDatabaseURL(t *testing.T) {
	for _, k := range []string{"TG_API_ID", "TG_API_HASH", "TG_PHONE"} {
		t.Setenv(k, "")
		require.NoError(t, os.Unsetenv(k))
	}
	t.Setenv("DATABASE_URL", "postgres://u:CANARY-pw@127.0.0.1:1/db?connect_timeout=2")
	var stdout, stderr bytes.Buffer

	code := run(context.Background(), []string{"migrate", "status"}, &stdout, &stderr)

	assert.Equal(t, 1, code, "unreachable database")
	assert.Contains(t, stderr.String(), `"msg":"migrate failed"`)
	assert.NotContains(t, stderr.String(), "config invalid")
	assert.NotContains(t, stderr.String(), "TG_API")
	assert.NotContains(t, stderr.String(), "CANARY")
}
