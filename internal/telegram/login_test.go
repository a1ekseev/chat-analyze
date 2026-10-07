package telegram

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaskPhone(t *testing.T) {
	tests := map[string]string{
		"+79991234567": "+7******4567",
		"+12025550123": "+1******0123",
		"+1234":        "+****",
		"79991234567":  "7******4567",
		"":             "",
	}
	for in, want := range tests {
		assert.Equal(t, want, MaskPhone(in), in)
	}
}

func TestPrompter_ReadsLinesFromNonTerminal(t *testing.T) {
	var out bytes.Buffer
	p := &prompter{in: bufio.NewReader(strings.NewReader("12345\n  secret pass \r\n")), out: &out}

	code, err := p.ask("Code: ")
	require.NoError(t, err)
	assert.Equal(t, "12345", code)

	pass, err := p.ask("Password: ")
	require.NoError(t, err)
	assert.Equal(t, "  secret pass ", pass, "password kept verbatim except the line ending")
	assert.Equal(t, "Code: Password: ", out.String())

	_, err = p.ask("More: ")
	require.Error(t, err, "EOF")
}

func TestUserAuth_TrimsCodeOnly(t *testing.T) {
	a := userAuth{p: &prompter{in: bufio.NewReader(strings.NewReader(" 12345 \n pw \n")), out: &bytes.Buffer{}}}
	code, err := a.Code(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, "12345", code)
	pw, err := a.Password(context.Background())
	require.NoError(t, err)
	assert.Equal(t, " pw ", pw)
}
