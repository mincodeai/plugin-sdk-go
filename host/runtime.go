package host

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
)

// Shell (manifest capability "shell"): the command runs through the system
// shell with the plugin directory as the default working directory.

// ShellResult is the outcome of Exec.
type ShellResult struct {
	Success  bool   `json:"success"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
}

// ShellOptions configures Exec. Cwd is relative to the plugin directory;
// TimeoutSecs defaults to 30 and is capped at 300 by the host.
type ShellOptions struct {
	Cwd         string
	TimeoutSecs int
}

// Exec runs command; a non-zero exit is reported in the result, not as an error.
func Exec(ctx context.Context, c Caller, command string, options ShellOptions) (ShellResult, error) {
	raw, err := call(ctx, c, "host.shell.exec", map[string]any{"command": command, "cwd": options.Cwd, "timeoutSecs": options.TimeoutSecs})
	if err != nil {
		return ShellResult{}, err
	}
	// Older hosts encoded the result as a JSON string.
	var encoded string
	if json.Unmarshal(raw, &encoded) == nil {
		raw = json.RawMessage(encoded)
	}
	var result ShellResult
	return result, decodeInto(raw, &result)
}

// SetContext publishes an in-memory command context value (manifest
// capability "commands") used by `when` expressions; value is a string,
// number, bool or nil and is never persisted.
func SetContext(ctx context.Context, c Caller, key string, value any) error {
	_, err := call(ctx, c, "host.context.set", map[string]any{"key": key, "value": value})
	return err
}

// Info identifies the host.
type Info struct {
	Name                   string `json:"name"`
	RuntimeProtocolVersion int    `json:"runtimeProtocolVersion"`
	ManifestAPIVersion     int    `json:"manifestApiVersion"`
	Platform               string `json:"platform"`
}

// HostInfo returns the host name, protocol versions and platform.
func HostInfo(ctx context.Context, c Caller) (Info, error) {
	raw, err := call(ctx, c, "host.info", map[string]any{})
	if err != nil {
		return Info{}, err
	}
	var info Info
	return info, decodeInto(raw, &info)
}

// Description is the host.api.describe result: the methods and events this
// plugin may use with their availability, plus host limits.
type Description struct {
	Methods []struct {
		Name       string `json:"name"`
		Capability string `json:"capability,omitempty"`
		Available  bool   `json:"available"`
	} `json:"methods"`
	Events []struct {
		Name       string `json:"name"`
		Capability string `json:"capability,omitempty"`
		Available  bool   `json:"available"`
	} `json:"events"`
	Features map[string]int `json:"features"`
	Limits   map[string]int `json:"limits"`
}

// Available reports whether a host method is available to this plugin.
func (d Description) Available(method string) bool {
	for _, entry := range d.Methods {
		if entry.Name == method {
			return entry.Available
		}
	}
	return false
}

// Describe returns the host's API description for feature detection.
func Describe(ctx context.Context, c Caller) (Description, error) {
	raw, err := call(ctx, c, "host.api.describe", map[string]any{})
	if err != nil {
		return Description{}, err
	}
	var description Description
	return description, decodeInto(raw, &description)
}

// Log writes to the plugin log in MinCode (level debug, info, warn or error).
// Never log credentials or full sensitive payloads.
func Log(ctx context.Context, c Caller, level, message string) error {
	_, err := call(ctx, c, "host.log", map[string]string{"level": level, "message": message})
	return err
}

// Workspace files (manifest capabilities "workspace.read" and
// "workspace.write"). Paths are relative to the active workspace root; VCS
// internals and credential files are never accessible.

// WorkspaceReadChunkBytes and WorkspaceWriteMaxBytes mirror the host limits.
const (
	WorkspaceReadChunkBytes = 512 * 1024
	WorkspaceWriteMaxBytes  = 512 * 1024
)

// WorkspaceEntry describes a workspace file or directory; Type is file, dir,
// symlink or other; ModifiedAt is Unix milliseconds.
type WorkspaceEntry struct {
	Path       string `json:"path"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	Size       int64  `json:"size"`
	ModifiedAt int64  `json:"modifiedAt"`
	SHA256     string `json:"sha256,omitempty"`
}

// WorkspaceErrorCode returns the machine-readable code of a workspace error
// (no_root, not_found, denied, conflict, too_large, not_file, not_directory,
// invalid_path, io) or "".
func WorkspaceErrorCode(err error) string {
	var hostErr *Error
	if !errors.As(err, &hostErr) || len(hostErr.Data) == 0 {
		return ""
	}
	var data struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(hostErr.Data, &data)
	return data.Code
}

// WorkspaceRoot returns the active workspace root, or "" when none is open.
func WorkspaceRoot(ctx context.Context, c Caller) (string, error) {
	raw, err := call(ctx, c, "host.workspace.root", map[string]any{})
	if err != nil {
		return "", err
	}
	var result struct {
		Root *string `json:"root"`
	}
	if err := decodeInto(raw, &result); err != nil || result.Root == nil {
		return "", err
	}
	return *result.Root, nil
}

// ListWorkspace lists a directory ("" is the root). truncated is true when the
// host capped the listing.
func ListWorkspace(ctx context.Context, c Caller, dir string) (entries []WorkspaceEntry, truncated bool, err error) {
	raw, err := call(ctx, c, "host.workspace.list", map[string]string{"path": dir})
	if err != nil {
		return nil, false, err
	}
	var result struct {
		Entries   []WorkspaceEntry `json:"entries"`
		Truncated bool             `json:"truncated"`
	}
	return result.Entries, result.Truncated, decodeInto(raw, &result)
}

// StatWorkspace describes one entry; withSHA256 also hashes a regular file.
func StatWorkspace(ctx context.Context, c Caller, path string, withSHA256 bool) (WorkspaceEntry, error) {
	raw, err := call(ctx, c, "host.workspace.stat", map[string]any{"path": path, "sha256": withSHA256})
	if err != nil {
		return WorkspaceEntry{}, err
	}
	var entry WorkspaceEntry
	return entry, decodeInto(raw, &entry)
}

// ReadWorkspaceFile reads a whole workspace file; files larger than maxBytes
// fail (maxBytes <= 0 means 16 MiB).
func ReadWorkspaceFile(ctx context.Context, c Caller, path string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = 16 * 1024 * 1024
	}
	var data []byte
	var offset int64
	for {
		raw, err := call(ctx, c, "host.workspace.read", map[string]any{"path": path, "offset": offset, "maxBytes": WorkspaceReadChunkBytes})
		if err != nil {
			return nil, err
		}
		var frame struct {
			Data       string `json:"data"`
			NextOffset int64  `json:"nextOffset"`
			EOF        bool   `json:"eof"`
			Size       int64  `json:"size"`
		}
		if err := decodeInto(raw, &frame); err != nil {
			return nil, err
		}
		if frame.Size > maxBytes {
			return nil, errors.New("workspace file exceeds maxBytes")
		}
		chunk, err := base64.StdEncoding.DecodeString(frame.Data)
		if err != nil {
			return nil, errors.New("host returned invalid workspace file data")
		}
		data = append(data, chunk...)
		offset = frame.NextOffset
		if frame.EOF {
			return data, nil
		}
		if len(chunk) == 0 {
			return nil, errors.New("host returned an empty non-terminal workspace frame")
		}
	}
}

// WorkspaceWriteOptions configures WriteWorkspaceFile. ExpectedSHA256 (from
// StatWorkspace) rejects the write when the file changed; CreateOnly rejects
// it when the file exists; CreateParents creates missing directories.
type WorkspaceWriteOptions struct {
	ExpectedSHA256 string
	CreateOnly     bool
	CreateParents  bool
}

// WorkspaceWriteResult reports a completed write.
type WorkspaceWriteResult struct {
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
	Created bool   `json:"created"`
}

// WriteWorkspaceFile atomically replaces one file (at most 512 KiB).
func WriteWorkspaceFile(ctx context.Context, c Caller, path string, data []byte, options WorkspaceWriteOptions) (WorkspaceWriteResult, error) {
	if len(data) > WorkspaceWriteMaxBytes {
		return WorkspaceWriteResult{}, errors.New("workspace writes are limited to 512 KiB")
	}
	params := map[string]any{"path": path, "data": base64.StdEncoding.EncodeToString(data), "createParents": options.CreateParents}
	switch {
	case options.CreateOnly:
		params["expectedSha256"] = ""
	case options.ExpectedSHA256 != "":
		params["expectedSha256"] = options.ExpectedSHA256
	}
	raw, err := call(ctx, c, "host.workspace.write", params)
	if err != nil {
		return WorkspaceWriteResult{}, err
	}
	var result WorkspaceWriteResult
	return result, decodeInto(raw, &result)
}

// Host events. Subscribe with the rpc Options.Events field (or
// SubscribeEvents) and receive them in Options.OnEvent. Events that carry
// paths need workspace.read; they reach only a running plugin.
const (
	EventWorkspaceChanged     = "workspace.changed"
	EventWorkspaceRootChanged = "workspace.rootChanged"
	EventActiveFileChanged    = "editor.activeFileChanged"
	EventFileSaved            = "editor.fileSaved"
	// EventSelectionChanged carries { path, selection: { startLine, startColumn, endLine, endColumn } }.
	EventSelectionChanged = "editor.selectionChanged"
	// EventGitHeadChanged carries { branch?, detached, commit? } for the workspace root.
	EventGitHeadChanged = "git.headChanged"
	// EventAIActivity (capability ai.activity) carries { seq, runId?, source?:
	// "assistant" | "agent", phase?, active, thinking, working, waiting }: a
	// snapshot of unfinished AI tasks numbered by seq (keep the highest; events
	// may arrive out of order), sent on every run phase change (with that run)
	// and every minute while tasks are active. It never contains conversation content.
	EventAIActivity = "ai.activity"
	// EventAITasks (capability ai.tasks) carries { seq, tasks: [{ runId,
	// source: "assistant" | "agent", phase, title, summary?, updatedAt }] }:
	// the top-level AI tasks that are unfinished or finished within the last
	// 10 minutes (at most 20, newest first, title <= 80 and summary <= 160
	// characters), numbered by seq (keep the highest). Sent shortly after any
	// change and every minute while tasks are listed. It never contains
	// conversation content, tool details or session/workspace identifiers.
	EventAITasks = "ai.tasks"
)

// SubscribeEvents replaces this process's subscriptions ([] unsubscribes) and
// returns the accepted events.
func SubscribeEvents(ctx context.Context, c Caller, events []string) ([]string, error) {
	if events == nil {
		events = []string{}
	}
	raw, err := call(ctx, c, "host.events.subscribe", map[string]any{"events": events})
	if err != nil {
		return nil, err
	}
	var result struct {
		Events []string `json:"events"`
	}
	return result.Events, decodeInto(raw, &result)
}
