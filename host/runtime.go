package host

import (
	"bytes"
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
	if value := bytes.TrimSpace(raw); len(value) == 0 || value[0] != '{' {
		return Description{}, errors.New("host.api.describe result must be an object")
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

// SearchProviderPayload is the search action input. Project is present only
// after SearchFileTargets negotiation and a ready host scope.
type SearchProviderPayload struct {
	Provider string         `json:"provider"`
	Query    string         `json:"query"`
	Locale   string         `json:"locale"`
	Project  *SearchProject `json:"project,omitempty"`
}
type SearchProject struct {
	Name string `json:"name"`
	Root string `json:"root"`
}

// SearchResultTarget preserves view/command/url targets and adds file targets.
// A file path is clean, slash-separated and root-relative. Line/Column are
// optional 1-based positions (column requires line), at most 10,000,000.
type SearchResultTarget struct {
	Type    string         `json:"type"`
	View    string         `json:"view,omitempty"`
	Command string         `json:"command,omitempty"`
	Args    map[string]any `json:"args,omitempty"`
	URL     string         `json:"url,omitempty"`
	Path    string         `json:"path,omitempty"`
	Line    *int           `json:"line,omitempty"`
	Column  *int           `json:"column,omitempty"`
}
type SearchResultItem struct {
	ID     string             `json:"id"`
	Title  string             `json:"title"`
	Detail string             `json:"detail,omitempty"`
	Target SearchResultTarget `json:"target"`
}

// SearchFileScope and ProviderRuntime are host-owned. Providers return only
// items and must never invent scopes or identities. Revision, Instance and
// Generation are canonical decimal strings, not floating-point numbers.
type SearchFileScope struct {
	WorkspaceID string `json:"workspaceId"`
	Root        string `json:"root"`
	Revision    string `json:"revision"`
}
type ProviderRuntime struct {
	Instance   string `json:"instance"`
	Generation string `json:"generation"`
}

// SearchResponse is the host-normalized response, not a provider's result.
type SearchResponse struct {
	Items           []SearchResultItem `json:"items"`
	Dropped         int                `json:"dropped"`
	Truncated       bool               `json:"truncated"`
	FileScope       *SearchFileScope   `json:"fileScope,omitempty"`
	ProviderRuntime *ProviderRuntime   `json:"providerRuntime,omitempty"`
}

// WorkspaceFileBatch is fileCreated/fileDeleted (Paths) or fileRenamed
// (Renames). Origin is host or watcher. ScopeRevision remains a string.
type WorkspaceFileBatch struct {
	Paths          []string          `json:"paths,omitempty"`
	Renames        []WorkspaceRename `json:"renames,omitempty"`
	ScopeRevision  string            `json:"scopeRevision"`
	Origin         string            `json:"origin"`
	RescanRequired bool              `json:"rescanRequired"`
}
type WorkspaceRename struct {
	OldPath string `json:"oldPath"`
	NewPath string `json:"newPath"`
}

// WorkspaceRescanData.Reason is overflow, truncated, sourceLost or deliveryLost.
type WorkspaceRescanData struct {
	ScopeRevision string `json:"scopeRevision"`
	Reason        string `json:"reason"`
}
type GitStatusData struct {
	ScopeRevision string `json:"scopeRevision"`
	Staged        int    `json:"staged"`
	Unstaged      int    `json:"unstaged"`
	Untracked     int    `json:"untracked"`
	Conflicted    int    `json:"conflicted"`
}
type AppLockData struct {
	Locked bool `json:"locked"`
}

// App suspend/resume data is an empty JSON object. Availability must be
// checked through Describe; a feature offer does not confirm a native source.

// Host events. Subscribe with the rpc Options.Events field (or
// SubscribeEvents) and receive them in Options.OnEvent. Events that carry
// paths need workspace.read; they reach only a running plugin.
const (
	EventWorkspaceChanged     = "workspace.changed"
	EventWorkspaceRootChanged = "workspace.rootChanged"
	EventActiveFileChanged    = "editor.activeFileChanged"
	EventFileSaved            = "editor.fileSaved"
	// File/Git events need workspace.read. Check Describe.Events availability;
	// feature offers do not imply an event source is available.
	EventFileCreated      = "workspace.fileCreated"
	EventFileDeleted      = "workspace.fileDeleted"
	EventFileRenamed      = "workspace.fileRenamed"
	EventRescanRequired   = "workspace.rescanRequired"
	EventGitStatusChanged = "git.statusChanged"
	// Lock notifications are hints, not an authorization or unlock API.
	EventAppLockChanged = "app.lockChanged"
	// Power events are unavailable until the host confirms a real source.
	EventAppSuspend = "app.suspend"
	EventAppResume  = "app.resume"
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
	// EventTestRunEnded (capability workspace.read, host 0.3.0) carries
	// TestRunEndedData { runId, reason }: the debug session of a streaming
	// test run ended (see PublishTestEvents). It is sent to the run's plugin
	// only, whether or not it subscribed; stop listening and clean up.
	EventTestRunEnded = "tests.runEnded"
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
