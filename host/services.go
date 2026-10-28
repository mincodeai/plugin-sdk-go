package host

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// Services (host.services.*): call capabilities other plugins provide, and
// provide them. Callers need the plugins capability; providers need nothing
// extra. See docs/development/plugin-development.md and
// wails-app/backend/plugin/host_services.go for the host side.
//
// Calling: InvokeService waits for the result; StartService + WaitService
// report progress and can be cancelled with CancelService. File inputs come
// from the caller's workspace (workspace.read), its own file storage, or an
// upload (UploadServiceFile). File outputs are tokens: ReadServiceFile,
// SaveServiceFile (into the caller's storage) or ReleaseServiceFile.
//
// Providing: the backing action receives the envelope DecodeServiceRequest
// decodes; it reads input files with ReadServiceFile, writes output files
// with CreateServiceFile and returns TextResult, JSONResult or FilesResult
// (or struct{}{} for a service without output). A provider that negotiated
// tasks reports progress with rpc.ReportProgress(ctx, ServiceProgress{...}).

const (
	// ServiceChunkBytes is the largest chunk of a service file read, upload
	// or create.
	ServiceChunkBytes = 512 * 1024
	// ServiceFileMaxBytes is the largest service file.
	ServiceFileMaxBytes = 64 * 1024 * 1024
	// serviceMaxExecTimeout is the longest an entry may run (its timeoutMs).
	serviceMaxExecTimeout = 300 * time.Second
	// serviceFileSaveTimeout bounds host.services.file.save, which copies up
	// to ServiceFileMaxBytes inside the host.
	serviceFileSaveTimeout = 60 * time.Second
)

// serviceWaitDelays are WaitService's polling delays; the last repeats.
var serviceWaitDelays = []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second}

// ServiceShape is a provider's input or output shape.
type ServiceShape struct {
	Kind     string   `json:"kind"`
	Accepts  []string `json:"accepts,omitempty"`
	MaxBytes int64    `json:"maxBytes,omitempty"`
	MaxFiles int      `json:"maxFiles,omitempty"`
	Mime     string   `json:"mime,omitempty"`
}

// ServiceProvider is one plugin entry offering a service.
type ServiceProvider struct {
	PluginID       string            `json:"pluginId"`
	PluginName     string            `json:"pluginName"`
	EntryID        string            `json:"entryId"`
	Title          string            `json:"title"`
	TitleLocalized map[string]string `json:"titleLocalized,omitempty"`
	Description    string            `json:"description,omitempty"`
	Input          ServiceShape      `json:"input"`
	Output         ServiceShape      `json:"output"`
	Params         map[string]any    `json:"params,omitempty"`
	Risk           string            `json:"risk"`
	IsDefault      bool              `json:"isDefault"`
}

// ServiceInfo is a service and the providers this plugin may call.
type ServiceInfo struct {
	Service   string            `json:"service"`
	Providers []ServiceProvider `json:"providers"`
}

// ServiceProviderRef names a provider; an empty EntryID lets the host pick
// the plugin's entry for the service.
type ServiceProviderRef struct {
	PluginID string `json:"pluginId"`
	EntryID  string `json:"entryId,omitempty"`
}

// ServiceFileSource is one input file: {Source: "workspace", Path} (needs
// workspace.read), {Source: "storage", Path} (the caller's file storage) or
// {Source: "upload", Token} (from UploadServiceFile).
type ServiceFileSource struct {
	Source string `json:"source"`
	Path   string `json:"path,omitempty"`
	Token  string `json:"token,omitempty"`
}

// ServiceCall is one call. Set Text for a text service, Files for a file
// service, neither for a service without input. Provider is optional (nil:
// the user's default, or the user picks). TimeoutMs bounds the approval and
// provider prompts (0: 25 s; 5000-300000). ExecTimeoutMs is SDK-only: how
// long InvokeService waits for the provider after the prompts (0: 300 s, the
// longest any entry may run); a ctx deadline also ends the wait.
type ServiceCall struct {
	Service       string
	Provider      *ServiceProviderRef
	Text          *string
	Files         []ServiceFileSource
	Params        map[string]any
	TimeoutMs     int
	ExecTimeoutMs int
}

// ServiceFile is a service file token with its metadata (never a path).
type ServiceFile struct {
	Token string `json:"token"`
	Name  string `json:"name"`
	Mime  string `json:"mime"`
	Size  int64  `json:"size"`
}

// ServiceResult is a successful call. Output is the JSON string of a text
// service, the value of a json service, the []ServiceFile of a file
// service, or null; decode it with Text, Files or Decode.
type ServiceResult struct {
	Provider ServiceProviderRef `json:"provider"`
	Output   json.RawMessage    `json:"output"`
}

// Text decodes a text service's output.
func (r ServiceResult) Text() (string, error) {
	var text string
	if err := json.Unmarshal(r.Output, &text); err != nil {
		return "", errors.New("service output is not text")
	}
	return text, nil
}

// Files decodes a file service's output files.
func (r ServiceResult) Files() ([]ServiceFile, error) {
	var files []ServiceFile
	if err := json.Unmarshal(r.Output, &files); err != nil {
		return nil, errors.New("service output is not a file list")
	}
	return files, nil
}

// Decode decodes a json service's output into v.
func (r ServiceResult) Decode(v any) error { return decodeInto(r.Output, v) }

// ServiceProgress is a call's progress: a bar when Total is set, otherwise
// indeterminate, with Message (≤ 200 characters) as plain text. Raw is the
// provider's progress as reported (≤ 4 KiB), extra fields included. A
// provider reports one with rpc.ReportProgress(ctx, ServiceProgress{...}).
type ServiceProgress struct {
	Completed *float64        `json:"completed,omitempty"`
	Total     *float64        `json:"total,omitempty"`
	Message   string          `json:"message,omitempty"`
	Raw       json.RawMessage `json:"-"`
}

// ServiceSnapshot is a started call's state: "running", "succeeded",
// "failed" or "cancelled".
type ServiceSnapshot struct {
	CallID      string             `json:"callId"`
	State       string             `json:"state"`
	Provider    ServiceProviderRef `json:"provider"`
	Progress    *ServiceProgress   `json:"progress,omitempty"`
	RawProgress json.RawMessage    `json:"rawProgress,omitempty"`
	Output      json.RawMessage    `json:"output,omitempty"`
	Error       string             `json:"error,omitempty"`
	Unconfirmed bool               `json:"unconfirmed,omitempty"`
}

// ServiceCallError is a started call that failed or was cancelled. Code is
// the host's error code ("timeout", "plugin_error", "cancelled", ...).
// Unconfirmed means the provider's outcome is unknown: it may have had side
// effects or still be running.
type ServiceCallError struct {
	CallID      string
	State       string
	Code        string
	Message     string
	Unconfirmed bool
}

func (e *ServiceCallError) Error() string { return e.Message }

// ServiceErrorCode returns the service error code of err: a
// *ServiceCallError's Code, or the code a host error carries ("not_found",
// "too_large", "busy", "cancelled", ...). "" when there is none.
func ServiceErrorCode(err error) string {
	var callErr *ServiceCallError
	if errors.As(err, &callErr) {
		return callErr.Code
	}
	return WorkspaceErrorCode(err)
}

// ListServices calls host.services.list (plugins capability): every service
// with the providers this plugin may call, or only service when it is set.
func ListServices(ctx context.Context, c Caller, service string) ([]ServiceInfo, error) {
	params := map[string]any{}
	if service != "" {
		params["service"] = service
	}
	raw, err := call(ctx, c, "host.services.list", params)
	if err != nil {
		return nil, err
	}
	var result struct {
		Services []ServiceInfo `json:"services"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, errors.New("host returned an invalid service list")
	}
	return result.Services, nil
}

func serviceCallParams(call ServiceCall) (map[string]any, error) {
	if call.Service == "" {
		return nil, errors.New("service call requires a service")
	}
	if call.Text != nil && len(call.Files) > 0 {
		return nil, errors.New("service call takes text or files, not both")
	}
	params := map[string]any{"service": call.Service}
	if call.Provider != nil {
		params["provider"] = call.Provider
	}
	switch {
	case call.Text != nil:
		params["input"] = *call.Text
	case len(call.Files) > 0:
		params["input"] = call.Files
	}
	if len(call.Params) > 0 {
		params["params"] = call.Params
	}
	if call.TimeoutMs > 0 {
		params["timeoutMs"] = call.TimeoutMs
	}
	return params, nil
}

// InvokeService calls host.services.invoke (plugins capability) and waits
// for the result. The user may be asked to approve the call and to pick a
// provider; a declined call is a host error with code -32003. The host-call
// timeout covers the prompts (TimeoutMs), the provider's run (ExecTimeoutMs,
// default the 300 s maximum) and a 5 s margin. Use StartService for progress
// or cancellation.
func InvokeService(ctx context.Context, c Caller, call ServiceCall) (ServiceResult, error) {
	if c == nil {
		return ServiceResult{}, ErrUnavailable
	}
	params, err := serviceCallParams(call)
	if err != nil {
		return ServiceResult{}, err
	}
	exec := serviceMaxExecTimeout
	if call.ExecTimeoutMs > 0 {
		exec = time.Duration(call.ExecTimeoutMs) * time.Millisecond
	}
	raw, err := c.Call(WithCallTimeout(ctx, promptWait(call.TimeoutMs)+exec+replyMargin), "host.services.invoke", params)
	if err != nil {
		return ServiceResult{}, err
	}
	var result ServiceResult
	if err := json.Unmarshal(raw, &result); err != nil || result.Provider.PluginID == "" {
		return ServiceResult{}, errors.New("host returned an invalid service result")
	}
	return result, nil
}

// StartService calls host.services.start (plugins capability). It returns
// the call ID once the prompts are done and the call runs; follow it with
// WaitService, ServiceStatus or CancelService.
func StartService(ctx context.Context, c Caller, call ServiceCall) (string, error) {
	if c == nil {
		return "", ErrUnavailable
	}
	params, err := serviceCallParams(call)
	if err != nil {
		return "", err
	}
	raw, err := c.Call(WithCallTimeout(ctx, promptWait(call.TimeoutMs)+replyMargin), "host.services.start", params)
	if err != nil {
		return "", err
	}
	var result struct {
		CallID string `json:"callId"`
	}
	if err := json.Unmarshal(raw, &result); err != nil || result.CallID == "" {
		return "", errors.New("host returned an invalid service call")
	}
	return result.CallID, nil
}

func decodeServiceSnapshot(raw json.RawMessage) (ServiceSnapshot, error) {
	var snapshot ServiceSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil || snapshot.State == "" {
		return ServiceSnapshot{}, errors.New("host returned an invalid service call snapshot")
	}
	if snapshot.Progress != nil && len(snapshot.RawProgress) > 0 {
		snapshot.Progress.Raw = snapshot.RawProgress
	}
	return snapshot, nil
}

// ServiceStatus calls host.services.status for a call this plugin started.
// Snapshots stay readable for 5 minutes after the call ends.
func ServiceStatus(ctx context.Context, c Caller, callID string) (ServiceSnapshot, error) {
	if callID == "" {
		return ServiceSnapshot{}, errors.New("service call id is required")
	}
	raw, err := call(ctx, c, "host.services.status", map[string]any{"callId": callID})
	if err != nil {
		return ServiceSnapshot{}, err
	}
	return decodeServiceSnapshot(raw)
}

// CancelService calls host.services.cancel. Cancelling is not rollback: the
// call may end failed with Unconfirmed when the provider did not confirm.
func CancelService(ctx context.Context, c Caller, callID string) error {
	if callID == "" {
		return errors.New("service call id is required")
	}
	_, err := call(ctx, c, "host.services.cancel", map[string]any{"callId": callID})
	return err
}

// serviceSnapshotResult turns a finished snapshot into the call's result.
func serviceSnapshotResult(snapshot ServiceSnapshot) (ServiceResult, error) {
	switch snapshot.State {
	case "succeeded":
		output := snapshot.Output
		if len(output) == 0 {
			output = json.RawMessage("null")
		}
		return ServiceResult{Provider: snapshot.Provider, Output: output}, nil
	case "failed", "cancelled":
		failure := &ServiceCallError{CallID: snapshot.CallID, State: snapshot.State, Code: "cancelled", Message: "the service call was cancelled", Unconfirmed: snapshot.Unconfirmed}
		if snapshot.State == "failed" {
			failure.Code, failure.Message = "plugin_error", snapshot.Error
			if prefix, _, found := strings.Cut(snapshot.Error, ":"); found && prefix != "" && !strings.ContainsAny(prefix, " \t") {
				failure.Code = prefix
			}
		}
		return ServiceResult{}, failure
	}
	return ServiceResult{}, fmt.Errorf("unknown service call state %q", snapshot.State)
}

// WaitService polls a started call (after 250 ms, 500 ms, then every
// second) until it ends, calling onProgress (optional) each time its
// progress changes. A failed or cancelled call returns a *ServiceCallError.
// Cancelling ctx stops the wait, not the call: use CancelService (with a
// live context) for that.
func WaitService(ctx context.Context, c Caller, callID string, onProgress func(ServiceProgress)) (ServiceResult, error) {
	if c == nil {
		return ServiceResult{}, ErrUnavailable
	}
	if callID == "" {
		return ServiceResult{}, errors.New("service call id is required")
	}
	var last []byte
	for attempt := 0; ; attempt++ {
		timer := time.NewTimer(serviceWaitDelays[min(attempt, len(serviceWaitDelays)-1)])
		select {
		case <-ctx.Done():
			timer.Stop()
			return ServiceResult{}, ctx.Err()
		case <-timer.C:
		}
		snapshot, err := ServiceStatus(ctx, c, callID)
		if err != nil {
			return ServiceResult{}, err
		}
		if onProgress != nil && snapshot.Progress != nil {
			key, _ := json.Marshal(snapshot.Progress)
			key = append(key, snapshot.RawProgress...)
			if !bytes.Equal(key, last) {
				last = key
				onProgress(*snapshot.Progress)
			}
		}
		if snapshot.State != "running" {
			return serviceSnapshotResult(snapshot)
		}
	}
}

// writeServiceChunks sends r in chunks of at most ServiceChunkBytes (and the
// host's maxChunkBytes) to an upload or create session.
func writeServiceChunks(ctx context.Context, c Caller, method, session string, hostChunk int, r io.Reader) error {
	chunk := ServiceChunkBytes
	if hostChunk > 0 && hostChunk < chunk {
		chunk = hostChunk
	}
	buf := make([]byte, chunk)
	var offset int64
	for {
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			if offset+int64(n) > ServiceFileMaxBytes {
				return fmt.Errorf("service file exceeds %d bytes", ServiceFileMaxBytes)
			}
			if _, err := call(ctx, c, method, map[string]any{"session": session, "offset": offset, "data": base64.StdEncoding.EncodeToString(buf[:n])}); err != nil {
				return err
			}
			offset += int64(n)
		}
		switch {
		case readErr == io.EOF || readErr == io.ErrUnexpectedEOF:
			return nil
		case readErr != nil:
			return readErr
		}
	}
}

type serviceSession struct {
	Session       string `json:"session"`
	MaxChunkBytes int    `json:"maxChunkBytes"`
}

func beginServiceSession(ctx context.Context, c Caller, method string, params map[string]any) (serviceSession, error) {
	raw, err := call(ctx, c, method, params)
	if err != nil {
		return serviceSession{}, err
	}
	var session serviceSession
	if err := json.Unmarshal(raw, &session); err != nil || session.Session == "" {
		return serviceSession{}, errors.New("host returned an invalid service file session")
	}
	return session, nil
}

func committedServiceToken(raw json.RawMessage) (string, error) {
	var file ServiceFile
	if err := json.Unmarshal(raw, &file); err != nil || file.Token == "" {
		return "", errors.New("host returned an invalid service file")
	}
	return file.Token, nil
}

// UploadServiceFile uploads r (≤ 64 MiB) for use as a {Source: "upload"}
// input and returns its token. name gives the file name and type the
// provider sees. Unused uploads expire on their own.
func UploadServiceFile(ctx context.Context, c Caller, name string, r io.Reader) (string, error) {
	if name == "" {
		return "", errors.New("service file name is required")
	}
	session, err := beginServiceSession(ctx, c, "host.services.file.upload.begin", map[string]any{})
	if err != nil {
		return "", err
	}
	if err := writeServiceChunks(ctx, c, "host.services.file.upload.append", session.Session, session.MaxChunkBytes, r); err != nil {
		return "", err
	}
	raw, err := call(ctx, c, "host.services.file.upload.commit", map[string]any{"session": session.Session, "name": name})
	if err != nil {
		return "", err
	}
	return committedServiceToken(raw)
}

// CreateServiceFile writes an output file of the running call callID (the
// envelope's callId) for a provider and returns its token for FilesResult.
func CreateServiceFile(ctx context.Context, c Caller, callID, name string, r io.Reader) (string, error) {
	if callID == "" || name == "" {
		return "", errors.New("service output file requires a call id and a name")
	}
	session, err := beginServiceSession(ctx, c, "host.services.file.create.begin", map[string]any{"callId": callID, "name": name})
	if err != nil {
		return "", err
	}
	if err := writeServiceChunks(ctx, c, "host.services.file.create.append", session.Session, session.MaxChunkBytes, r); err != nil {
		return "", err
	}
	raw, err := call(ctx, c, "host.services.file.create.commit", map[string]any{"session": session.Session})
	if err != nil {
		return "", err
	}
	return committedServiceToken(raw)
}

// ReadServiceFile copies the file token names to w in 512 KiB chunks: an
// input file for a provider, an output file for a caller.
func ReadServiceFile(ctx context.Context, c Caller, token string, w io.Writer) error {
	if token == "" {
		return errors.New("service file token is required")
	}
	var offset int64
	for {
		raw, err := call(ctx, c, "host.services.file.read", map[string]any{"token": token, "offset": offset, "length": ServiceChunkBytes})
		if err != nil {
			return err
		}
		var frame struct {
			Data string `json:"data"`
		}
		if err := json.Unmarshal(raw, &frame); err != nil || len(frame.Data) > base64.StdEncoding.EncodedLen(ServiceChunkBytes) {
			return errors.New("host returned an invalid service file chunk")
		}
		data, err := base64.StdEncoding.DecodeString(frame.Data)
		if err != nil {
			return errors.New("host returned an invalid service file chunk")
		}
		if offset+int64(len(data)) > ServiceFileMaxBytes {
			return fmt.Errorf("service file exceeds %d bytes", ServiceFileMaxBytes)
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		offset += int64(len(data))
		// The host returns full chunks until the end of the file.
		if len(data) < ServiceChunkBytes {
			return nil
		}
	}
}

// SaveServiceFile copies an output file into this plugin's file storage at
// storagePath (storage capability; counts toward the storage quota).
func SaveServiceFile(ctx context.Context, c Caller, token, storagePath string) error {
	if token == "" || storagePath == "" {
		return errors.New("saving a service file requires a token and a storage path")
	}
	_, err := call(WithCallTimeout(ctx, serviceFileSaveTimeout), c, "host.services.file.save", map[string]any{"token": token, "path": storagePath})
	return err
}

// ReleaseServiceFile frees a file before it expires (outputs live 10
// minutes).
func ReleaseServiceFile(ctx context.Context, c Caller, token string) error {
	if token == "" {
		return errors.New("service file token is required")
	}
	_, err := call(ctx, c, "host.services.file.release", map[string]any{"token": token})
	return err
}

// ServiceFileRef is an input file a provider receives.
type ServiceFileRef = ServiceFile

// ServiceCallerInfo is who called: Kind "plugin" (PluginID) or "host"
// (Surface). Record it if useful; the host already decided who may call.
type ServiceCallerInfo struct {
	Kind     string `json:"kind"`
	PluginID string `json:"pluginId,omitempty"`
	Surface  string `json:"surface,omitempty"`
}

// ServiceRequest is the payload a provider's action receives. Text is set
// for a text service, Files for a file service; Params holds the validated
// params with defaults filled in. CallID is needed by CreateServiceFile.
type ServiceRequest struct {
	CallID  string
	Service string
	Entry   string
	Text    string
	Files   []ServiceFileRef
	Params  json.RawMessage
	Caller  ServiceCallerInfo
}

// DecodeServiceRequest decodes a provider action's payload.
func DecodeServiceRequest(payload json.RawMessage) (ServiceRequest, error) {
	var envelope struct {
		CallID  string            `json:"callId"`
		Service string            `json:"service"`
		Entry   string            `json:"entry"`
		Input   json.RawMessage   `json:"input"`
		Params  json.RawMessage   `json:"params"`
		Caller  ServiceCallerInfo `json:"caller"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil || envelope.CallID == "" || envelope.Service == "" || envelope.Entry == "" {
		return ServiceRequest{}, errors.New("invalid service request")
	}
	request := ServiceRequest{CallID: envelope.CallID, Service: envelope.Service, Entry: envelope.Entry, Params: envelope.Params, Caller: envelope.Caller}
	if isNull(request.Params) {
		request.Params = json.RawMessage(`{}`)
	}
	input := bytes.TrimSpace(envelope.Input)
	switch {
	case isNull(input):
	case input[0] == '"':
		if err := json.Unmarshal(input, &request.Text); err != nil {
			return ServiceRequest{}, errors.New("invalid service request input")
		}
	case input[0] == '[':
		if err := json.Unmarshal(input, &request.Files); err != nil {
			return ServiceRequest{}, errors.New("invalid service request input")
		}
	default:
		return ServiceRequest{}, errors.New("invalid service request input")
	}
	return request, nil
}

// TextResult is a text service's result.
func TextResult(text string) any { return map[string]any{"text": text} }

// JSONResult is a json service's result.
func JSONResult(value any) any { return map[string]any{"json": value} }

// FilesResult is a file service's result: tokens from CreateServiceFile.
func FilesResult(tokens ...string) any {
	if tokens == nil {
		tokens = []string{}
	}
	return map[string]any{"files": tokens}
}
