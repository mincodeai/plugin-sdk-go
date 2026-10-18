package dbdriver

import (
	"encoding/json"
	"fmt"
	"log/slog"
)

// Redacted is what a Secret prints, marshals and logs as.
const Redacted = "[redacted]"

// Secret holds a credential (password, token) received in plugin.db.connect.
// It never prints: String, GoString, every fmt verb, MarshalJSON, MarshalText
// and slog all produce "[redacted]". Call Reveal only where the driver hands
// the value to the database client, and never log, persist or send it
// anywhere else.
type Secret struct {
	// A pointer keeps the value out of reflection-based printing even where
	// fmt cannot call the methods (for example inside unexported fields).
	p *string
}

// NewSecret wraps a credential.
func NewSecret(v string) Secret {
	if v == "" {
		return Secret{}
	}
	return Secret{p: &v}
}

// Reveal returns the plaintext.
func (s Secret) Reveal() string {
	if s.p == nil {
		return ""
	}
	return *s.p
}

// IsEmpty reports whether no credential was sent.
func (s Secret) IsEmpty() bool { return s.p == nil || *s.p == "" }

// String returns "[redacted]" (or "" for an empty secret).
func (s Secret) String() string {
	if s.IsEmpty() {
		return ""
	}
	return Redacted
}

// GoString returns "[redacted]".
func (s Secret) GoString() string { return quote(Redacted) }

// Format prints "[redacted]" for every verb.
func (s Secret) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(Redacted)) }

// MarshalJSON writes "[redacted]".
func (s Secret) MarshalJSON() ([]byte, error) { return []byte(`"` + Redacted + `"`), nil }

// MarshalText writes [redacted].
func (s Secret) MarshalText() ([]byte, error) { return []byte(Redacted), nil }

// LogValue keeps the secret out of log/slog output.
func (s Secret) LogValue() slog.Value { return slog.StringValue(Redacted) }

// UnmarshalJSON reads a JSON string (or null).
func (s *Secret) UnmarshalJSON(b []byte) error {
	var v *string
	if err := json.Unmarshal(b, &v); err != nil {
		return fmt.Errorf("secret must be a string")
	}
	if v == nil {
		*s = Secret{}
		return nil
	}
	*s = NewSecret(*v)
	return nil
}
