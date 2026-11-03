package host

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Generic host UI requests (wails-app/backend/plugin/host_ui.go). Navigation
// requests return as soon as the host accepted them; each plugin may send at
// most 20 per minute. Prompts wait for the user for TimeoutMs (default 25 s,
// 5000-300000) and a plugin can have one prompt open at a time; these helpers
// extend the host-call timeout to match. A prompt shown while handling a
// normal invoke must finish within that invoke's 30 s, so use a long task
// for longer waits.

const (
	defaultPromptTimeout = 25 * time.Second
	// replyMargin covers the round trip after the prompt closes.
	replyMargin = 5 * time.Second
	// pluginInvokeTimeout is the host's limit for the called action itself.
	pluginInvokeTimeout = 30 * time.Second
)

func promptWait(timeoutMs int) time.Duration {
	if timeoutMs <= 0 {
		return defaultPromptTimeout
	}
	return time.Duration(timeoutMs) * time.Millisecond
}

// EditorLocation names a file inside the active workspace root.
type EditorLocation struct {
	Path   string `json:"path"`
	Line   int    `json:"line,omitempty"`
	Column int    `json:"column,omitempty"`
}

// OpenEditor calls host.editor.open (workspace.read): the host opens the file
// in the project editor and moves the cursor to Line/Column when set.
func OpenEditor(ctx context.Context, c Caller, location EditorLocation) error {
	if c == nil {
		return ErrUnavailable
	}
	if location.Path == "" {
		return errors.New("editor location requires a path")
	}
	_, err := c.Call(ctx, "host.editor.open", location)
	return err
}

// ViewRequest opens one of the plugin's own views, or another plugin's view
// when PluginID is set. With Entry the view page receives an open request
// (source "plugin", from = the calling plugin) carrying Params: strings,
// numbers and booleans only, at most 16 keys.
//
// Raise also brings the MinCode main window to the front. It needs
// Activation, the single-use token the host delivered after one of the
// plugin's global shortcut actions ran (rpc.Options.OnGlobalShortcutActivation,
// valid for 120 s), and works only for the plugin's own tab view (PluginID
// empty or the plugin itself, not a floating view). Without a valid token the
// host refuses the request with error data code "activation_invalid" (and
// with "locked" while MinCode is locked); a request without Raise never moves
// the window.
type ViewRequest struct {
	PluginID   string         `json:"pluginId,omitempty"`
	ViewID     string         `json:"viewId"`
	Entry      string         `json:"entry,omitempty"`
	Params     map[string]any `json:"params,omitempty"`
	Raise      bool           `json:"raise,omitempty"`
	Activation string         `json:"activation,omitempty"`
}

// OpenView calls host.view.open (ui).
func OpenView(ctx context.Context, c Caller, request ViewRequest) error {
	if c == nil {
		return ErrUnavailable
	}
	if request.ViewID == "" {
		return errors.New("view request requires a viewId")
	}
	if request.Raise && request.Activation == "" {
		return errors.New("raising the main window requires an activation token")
	}
	_, err := c.Call(ctx, "host.view.open", request)
	return err
}

// Trigger says what started an action. A global shortcut press adds
// {"trigger": {"type": "globalShortcut", "id": "<entry id>"}} to the
// action's fixed payload.
type Trigger struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// TriggerFrom reads the trigger of an action payload (a JSON object, or a
// JSON string holding one). ok is false when the payload carries none.
func TriggerFrom(payload json.RawMessage) (Trigger, bool) {
	var text string
	if json.Unmarshal(payload, &text) == nil {
		payload = json.RawMessage(text)
	}
	var body struct {
		Trigger *Trigger `json:"trigger"`
	}
	if json.Unmarshal(payload, &body) != nil || body.Trigger == nil || body.Trigger.Type == "" || body.Trigger.ID == "" {
		return Trigger{}, false
	}
	return *body.Trigger, true
}

// DiffSide is one side of a diff: UTF-8 text of at most 256 KiB.
type DiffSide struct {
	Title   string `json:"title,omitempty"`
	Content string `json:"content"`
}

// DiffRequest is shown read-only by the host's diff editor.
type DiffRequest struct {
	Title    string   `json:"title"`
	Language string   `json:"language,omitempty"`
	Left     DiffSide `json:"left"`
	Right    DiffSide `json:"right"`
}

// OpenDiff calls host.diff.open (ui).
func OpenDiff(ctx context.Context, c Caller, request DiffRequest) error {
	if c == nil {
		return ErrUnavailable
	}
	if request.Title == "" {
		return errors.New("diff request requires a title")
	}
	_, err := c.Call(ctx, "host.diff.open", request)
	return err
}

// TerminalRequest opens a terminal tab. Cwd is relative to the workspace root
// (empty: the root). Command is one line; it runs only after the user
// confirms it in the host, otherwise the terminal opens without running it.
type TerminalRequest struct {
	Title   string `json:"title,omitempty"`
	Cwd     string `json:"cwd,omitempty"`
	Command string `json:"command,omitempty"`
}

// OpenTerminal calls host.terminal.open (shell).
func OpenTerminal(ctx context.Context, c Caller, request TerminalRequest) error {
	if c == nil {
		return ErrUnavailable
	}
	_, err := c.Call(ctx, "host.terminal.open", request)
	return err
}

// QuickPickItem is one choice; IDs are unique within a request.
type QuickPickItem struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
	Detail      string `json:"detail,omitempty"`
}

// QuickPickRequest lists 1-200 items. TimeoutMs is how long the user has
// (0: 25 s; 5000-300000).
type QuickPickRequest struct {
	Title       string          `json:"title,omitempty"`
	Placeholder string          `json:"placeholder,omitempty"`
	CanPickMany bool            `json:"canPickMany,omitempty"`
	Items       []QuickPickItem `json:"items"`
	TimeoutMs   int             `json:"timeoutMs,omitempty"`
}

// QuickPick calls host.quickPick (ui) and returns the chosen item IDs;
// cancelled is true when the user dismissed the prompt or it timed out.
func QuickPick(ctx context.Context, c Caller, request QuickPickRequest) (ids []string, cancelled bool, err error) {
	if c == nil {
		return nil, false, ErrUnavailable
	}
	raw, err := c.Call(WithCallTimeout(ctx, promptWait(request.TimeoutMs)+replyMargin), "host.quickPick", request)
	if err != nil {
		return nil, false, err
	}
	var result struct {
		Cancelled bool     `json:"cancelled"`
		IDs       []string `json:"ids"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, false, errors.New("host returned an invalid quick pick result")
	}
	return result.IDs, result.Cancelled, nil
}

// InputBoxRequest asks for one line of text (at most 4096 bytes). TimeoutMs
// is how long the user has (0: 25 s; 5000-300000).
type InputBoxRequest struct {
	Title       string `json:"title"`
	Prompt      string `json:"prompt,omitempty"`
	Placeholder string `json:"placeholder,omitempty"`
	Value       string `json:"value,omitempty"`
	Password    bool   `json:"password,omitempty"`
	TimeoutMs   int    `json:"timeoutMs,omitempty"`
}

// InputBox calls host.inputBox (ui).
func InputBox(ctx context.Context, c Caller, request InputBoxRequest) (value string, cancelled bool, err error) {
	if c == nil {
		return "", false, ErrUnavailable
	}
	raw, err := c.Call(WithCallTimeout(ctx, promptWait(request.TimeoutMs)+replyMargin), "host.inputBox", request)
	if err != nil {
		return "", false, err
	}
	var result struct {
		Cancelled bool   `json:"cancelled"`
		Value     string `json:"value"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", false, errors.New("host returned an invalid input box result")
	}
	return result.Value, result.Cancelled, nil
}

// OpenProject asks the host to confirm and open an absolute directory in
// Local Projects (ui). It returns when the request is accepted, not when
// the user confirms it; no workspace-read permission is granted.
func OpenProject(ctx context.Context, c Caller, path string) error {
	if c == nil {
		return ErrUnavailable
	}
	if path == "" {
		return errors.New("project directory is required")
	}
	_, err := c.Call(ctx, "host.project.open", map[string]string{"path": path})
	return err
}
