package host

import (
	"context"
	"encoding/json"
	"errors"
)

// GetConfiguration reads the calling plugin's effective settings from
// host.configuration.get (default -> global -> active-workspace override,
// including set secret settings). With keys, only those settings are
// returned; unset settings without a default are absent. Plugins cannot
// write their own settings: the user edits them in Settings > Plugins.
func GetConfiguration(ctx context.Context, c Caller, keys ...string) (map[string]json.RawMessage, error) {
	if c == nil {
		return nil, ErrUnavailable
	}
	params := map[string]any{}
	if len(keys) > 0 {
		params["keys"] = keys
	}
	raw, err := c.Call(ctx, "host.configuration.get", params)
	if err != nil {
		return nil, err
	}
	var result struct {
		Values map[string]json.RawMessage `json:"values"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	if result.Values == nil {
		return nil, errors.New("host.configuration.get returned no values")
	}
	return result.Values, nil
}

// DecodeConfiguration reads the settings and decodes them into target (a
// pointer to a struct with json tags matching the setting keys).
func DecodeConfiguration(ctx context.Context, c Caller, target any, keys ...string) error {
	values, err := GetConfiguration(ctx, c, keys...)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, target)
}
