package host

import (
	"context"
	"encoding/json"
)

// Notification is posted with Notify (manifest capability "notifications").
type Notification struct {
	Title string `json:"title"`
	Body  string `json:"body,omitempty"`
	// Level is info (default), success, warning or error.
	Level string `json:"level,omitempty"`
	// System asks for an operating-system notification even while MinCode is in
	// the foreground; otherwise one appears only when it is in the background.
	System bool `json:"system,omitempty"`
	// ViewID is the plugin view a click opens (default: the first view).
	ViewID string `json:"viewId,omitempty"`
	// Actions are up to 2 buttons shown on the notification in MinCode's
	// notification center (host 0.3.0; older hosts ignore them and still
	// deliver the notification). Each Command must be one of this plugin's
	// contributes.commands; an invalid action rejects the whole call.
	Actions []NotificationAction `json:"actions,omitempty"`
}

// NotificationAction is a notification button (host 0.3.0). ID matches
// ^[A-Za-z0-9._-]{1,32}$ and is unique in the notification; Title is a single
// line of at most 24 characters; Command is one of this plugin's commands,
// run without arguments.
type NotificationAction struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Command string `json:"command"`
}

// NotifyResult is the host's answer to NotifyWithResult.
type NotifyResult struct {
	Delivered bool `json:"delivered"`
	// Actions is how many buttons the host shows; nil on hosts older than
	// 0.3.0, which ignore Notification.Actions.
	Actions *int `json:"actions,omitempty"`
}

// Notify posts a notification (title ≤ 80, body ≤ 500 characters; at most 10 per minute).
func Notify(ctx context.Context, c Caller, notification Notification) error {
	if c == nil {
		return ErrUnavailable
	}
	_, err := c.Call(ctx, "host.notify", notification)
	return err
}

// NotifyWithResult is Notify that also returns the host's result, so a plugin
// can tell whether its notification actions are shown (host 0.3.0; older
// hosts ignore actions).
func NotifyWithResult(ctx context.Context, c Caller, notification Notification) (NotifyResult, error) {
	if c == nil {
		return NotifyResult{}, ErrUnavailable
	}
	raw, err := c.Call(ctx, "host.notify", notification)
	if err != nil {
		return NotifyResult{}, err
	}
	var result NotifyResult
	if len(raw) > 0 && string(raw) != "null" {
		err = json.Unmarshal(raw, &result)
	}
	return result, err
}

// ClipboardText is the result of ReadClipboard (manifest capability "clipboard").
type ClipboardText struct {
	Changed bool   `json:"changed"`
	Seq     int64  `json:"seq"`
	Text    string `json:"text"`
	// Concealed content (password managers, one-time codes) is never returned.
	Concealed bool `json:"concealed"`
	Truncated bool `json:"truncated"`
}

// ReadClipboard returns the clipboard text if it changed since seq (0 = always read).
func ReadClipboard(ctx context.Context, c Caller, since int64) (ClipboardText, error) {
	if c == nil {
		return ClipboardText{}, ErrUnavailable
	}
	raw, err := c.Call(ctx, "host.clipboard.read", map[string]int64{"since": since})
	if err != nil {
		return ClipboardText{}, err
	}
	var result ClipboardText
	err = json.Unmarshal(raw, &result)
	return result, err
}

// WriteClipboard replaces the clipboard text (UTF-8, ≤ 1 MiB).
func WriteClipboard(ctx context.Context, c Caller, text string) error {
	if c == nil {
		return ErrUnavailable
	}
	_, err := c.Call(ctx, "host.clipboard.write", map[string]string{"text": text})
	return err
}
