package config

import "log/slog"

const redacted = "[REDACTED]"

// Secret is a string that never reveals itself through fmt, slog or JSON.
// Use Reveal at the single point where the raw value is actually needed.
type Secret string

// String implements fmt.Stringer.
func (Secret) String() string { return redacted }

// GoString implements fmt.GoStringer (covers %#v).
func (Secret) GoString() string { return `"` + redacted + `"` }

// LogValue implements slog.LogValuer.
func (Secret) LogValue() slog.Value { return slog.StringValue(redacted) }

// MarshalJSON implements json.Marshaler.
func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }

// MarshalText implements encoding.TextMarshaler (covers text encoders and map keys).
func (Secret) MarshalText() ([]byte, error) { return []byte(redacted), nil }

// Reveal returns the raw secret value.
func (s Secret) Reveal() string { return string(s) }
