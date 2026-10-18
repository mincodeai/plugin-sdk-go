package host

import (
	"context"
	"errors"
)

// StatusBarUpdate changes one status bar item declared with "dynamic": true
// in contributes.statusBar. Nil fields keep their current value. Text is a
// single line of at most 48 characters, Tooltip at most 256, and Severity is
// "normal", "warning" or "error".
type StatusBarUpdate struct {
	ID       string  `json:"id"`
	Text     *string `json:"text,omitempty"`
	Tooltip  *string `json:"tooltip,omitempty"`
	Severity string  `json:"severity,omitempty"`
	Busy     *bool   `json:"busy,omitempty"`
	Visible  *bool   `json:"visible,omitempty"`
}

// UpdateStatusBarItem calls host.statusBar.update (commands capability). The
// host coalesces pushes, silently drops updates beyond 50 per second, and
// clears dynamic state when the plugin runtime ends.
func UpdateStatusBarItem(ctx context.Context, c Caller, update StatusBarUpdate) error {
	if c == nil {
		return ErrUnavailable
	}
	if update.ID == "" {
		return errors.New("status bar update requires an item id")
	}
	_, err := c.Call(ctx, "host.statusBar.update", update)
	return err
}

// ResetStatusBar calls host.statusBar.reset. An empty id restores every
// dynamic item of the plugin to its declared values.
func ResetStatusBar(ctx context.Context, c Caller, id string) error {
	if c == nil {
		return ErrUnavailable
	}
	params := map[string]string{}
	if id != "" {
		params["id"] = id
	}
	_, err := c.Call(ctx, "host.statusBar.reset", params)
	return err
}
