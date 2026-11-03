package host

import (
	"context"
	"encoding/json"
	"errors"
)

// RefreshDecorations calls host.decorations.refresh (host 0.3.0; needs the
// workspace.read capability and a contributes.fileDecorations provider). It
// is a data-free hint: the host asks the project tree to re-request this
// plugin's decorations for the directories it already shows, at most once
// every 2 seconds. scheduled is false when the hint was rate limited, and
// also when the host predates the method or the plugin declares no
// fileDecorations (-32601), so plugins can call it unconditionally.
func RefreshDecorations(ctx context.Context, c Caller) (scheduled bool, err error) {
	if c == nil {
		return false, ErrUnavailable
	}
	raw, err := c.Call(ctx, "host.decorations.refresh", struct{}{})
	if IsCode(err, -32601) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var result struct {
		Scheduled *bool `json:"scheduled"`
	}
	if err := json.Unmarshal(raw, &result); err != nil || result.Scheduled == nil {
		return false, errors.New("host.decorations.refresh returned an invalid result")
	}
	return *result.Scheduled, nil
}
