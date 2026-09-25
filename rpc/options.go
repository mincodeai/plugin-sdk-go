// Package rpc implements the stdio NDJSON JSON-RPC 2.0 loop shared by MinCode
// official process plugins (runtime protocol v1): plugin.initialize, ping,
// shutdown, invoke, the optional plugin.task.* feature and reverse host
// requests. Every observable wire behaviour that differed between plugins is
// an Options knob, so a plugin can migrate without changing a single byte of
// its protocol output; the presets (StrictDefaults, NodeDefaults) collect the
// two common families.
package rpc

import (
	"context"
	"encoding/json"
	"regexp"
	"time"

	"github.com/mincodeai/plugin-sdk-go/host"
)

// Error is a JSON-RPC error with an exact code and message. Handlers return it
// (or wrap it) to control the reply; any other error goes through
// Options.MapError.
type Error struct {
	Code    int
	Message string
	// Data, when set, is written verbatim as the error's "data" member (it
	// must be valid, compact JSON).
	Data json.RawMessage
}

func (e *Error) Error() string { return e.Message }

// Errorf-free constructors keep call sites short.

// NewError returns &Error{code, message}.
func NewError(code int, message string) *Error { return &Error{Code: code, Message: message} }

// Standard codes.
const (
	CodeParseError     = -32700
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeServerError    = -32000
	CodeBusy           = -32029
)

// Handler runs one invoke or task action. payload is a JSON object (see
// PayloadMode). A json.RawMessage or []byte result is written verbatim (it
// must be valid JSON); any other value is marshalled per the reply style.
// host.FromContext(ctx) is the reverse-request client; ReportProgress
// publishes task progress.
type Handler func(ctx context.Context, action string, payload json.RawMessage) (any, error)

// Style selects the byte layout of reply envelopes.
type Style int

const (
	// StyleGo encodes {"jsonrpc","id","result"|"error"} with encoding/json
	// (ids are echoed compacted; U+2028/2029 are escaped; HTML escaping per
	// Options.EscapeHTML).
	StyleGo Style = iota
	// StyleSorted encodes the envelope as a map: {"error"|"id","jsonrpc","result"}
	// with sorted keys (plugins that replied with map[string]any).
	StyleSorted
	// StyleJS writes the envelope like JSON.stringify in the Node runtime the
	// plugin was ported from: ids are re-serialised as JS values (1e3 → 1000)
	// and U+2028 stays literal.
	StyleJS
)

// IDPolicy decides which request ids get a reply. Requests without an id are
// notifications and are always ignored.
type IDPolicy int

const (
	// IDStrict answers string or number ids of at most MaxIDBytes raw bytes;
	// everything else is silently ignored.
	IDStrict IDPolicy = iota
	// IDNonNull answers any id except null.
	IDNonNull
	// IDAny answers any present id, including null.
	IDAny
)

// InvalidMode says what happens to a line that is not a usable request.
type InvalidMode int

const (
	// InvalidReply writes {"jsonrpc":"2.0","id":null,"error":{"code":-32700,...}}.
	InvalidReply InvalidMode = iota
	// InvalidStderr writes InvalidMessage to stderr and continues.
	InvalidStderr
)

// MethodTypeMode handles a request whose "method" is not a string.
type MethodTypeMode int

const (
	// MethodTypeInvalid treats the line as invalid (see InvalidMode).
	MethodTypeInvalid MethodTypeMode = iota
	// MethodTypeUnknown answers MethodNotFound.
	MethodTypeUnknown
	// MethodTypeIgnore treats the line as a (non-matching) host response.
	MethodTypeIgnore
	// MethodTypeTruthy follows JS truthiness (Node runtime `if (!msg.method)`):
	// a falsy method (false, 0, "", null) makes the line a host response and
	// any other non-string method answers MethodNotFound.
	MethodTypeTruthy
)

// PayloadMode selects how invoke params are decoded.
type PayloadMode int

const (
	// PayloadString: params must decode into {action string, payload string}
	// (else InvalidParams); an empty payload becomes "{}". The handler parses
	// the payload itself (see PayloadDecoder).
	PayloadString PayloadMode = iota
	// PayloadObject: params may be anything; action is the string "action"
	// member (else ""). payload may be absent, null, "", a JSON text string or
	// an object and is normalised to an object before the handler runs
	// (PayloadInvalidJSON / PayloadNotObject otherwise).
	PayloadObject
)

// Options configure Serve. The zero value is not useful; start from a preset.
type Options struct {
	// --- framing ---

	// MaxLineBytes bounds one input line (default 1 MiB).
	MaxLineBytes int
	// OverlongFatal stops the transport on an overlong line (bufio.Scanner
	// behaviour: a line of MaxLineBytes bytes or more without its newline);
	// otherwise the line (> MaxLineBytes after trimming CR/LF) is invalid.
	OverlongFatal bool
	// FatalLineBytes (without OverlongFatal, 0: unlimited) stops the transport
	// on a line that does not fit a bufio.Scanner buffer of this size (line
	// plus newline), e.g. a 4 MiB scanner in front of a 1 MiB MaxLineBytes.
	FatalLineBytes int
	// FatalMessage is written to stderr (plus newline) when Serve fails
	// (read/write error or fatal overlong line).
	FatalMessage string
	// StrictJSON requires jsonx.Strict (no duplicate keys, lone surrogates,
	// invalid UTF-8) for the whole line.
	StrictJSON bool
	// StructEnvelope decodes each line like the original Go plugins' request
	// struct (case-insensitive member names, a malformed "error" member makes
	// the line invalid) instead of as a plain member map.
	StructEnvelope bool
	// LenientErrorMember (with StructEnvelope) ignores the "error" member of
	// requests and decodes host replies apart as {result, error}: a reply whose
	// error member is malformed is dropped instead of making the line invalid
	// (plugins whose request struct had no result/error fields).
	LenientErrorMember bool
	// DecodeEnvelope, when set, replaces the envelope decoding (plain member
	// map or StructEnvelope): it returns the line's members or false for an
	// invalid line. A plugin with its own request struct (e.g. leetcode-cn:
	// unknown members, a missing id or method make the line invalid) keeps
	// its exact acceptance rules this way.
	DecodeEnvelope func(line []byte) (map[string]json.RawMessage, bool)
	// RequireVersion requires "jsonrpc":"2.0".
	RequireVersion bool
	// Invalid and InvalidMessage handle unusable lines.
	Invalid        InvalidMode
	InvalidMessage string
	// InvalidOmitID (InvalidReply) writes the -32700 reply without an "id"
	// member ({"jsonrpc":"2.0","error":{...}}) instead of "id":null.
	InvalidOmitID bool
	// SkipEmptyLines ignores zero-length lines; SkipBlankLines also ignores
	// whitespace-only lines.
	SkipEmptyLines bool
	SkipBlankLines bool
	// NonStringMethod handles "method" values that are not strings.
	NonStringMethod MethodTypeMode
	// IDs and MaxIDBytes (IDStrict only, default 256) filter request ids.
	IDs        IDPolicy
	MaxIDBytes int

	// --- replies ---

	Style Style
	// EscapeHTML escapes <, > and & like json.Marshal (StyleGo/StyleSorted).
	EscapeHTML bool

	// --- methods ---

	// MethodNotFound is the -32601 message for unknown methods.
	MethodNotFound string
	// RequireInitialize rejects invokes before plugin.initialize with
	// NotInitialized.
	RequireInitialize bool
	NotInitialized    *Error

	// --- invoke ---

	Handler Handler
	// MaxInflight bounds concurrent invokes (and tasks when
	// TaskOptions.SharedGate); excess requests get Busy.
	MaxInflight int
	Busy        *Error
	// ParamsBeforeBusy validates params before the busy check.
	ParamsBeforeBusy bool
	Payload          PayloadMode
	// KeepEmptyPayload (PayloadString): an empty payload string reaches the
	// handler as an empty RawMessage instead of "{}".
	KeepEmptyPayload bool
	// InvalidParams answers malformed invoke params (PayloadString).
	InvalidParams *Error
	// KnownAction, when set, rejects unknown actions before the handler runs
	// with UnknownAction(action).
	KnownAction   func(action string) bool
	UnknownAction func(action string) *Error
	// PayloadNotObject / PayloadInvalidJSON (PayloadObject).
	PayloadNotObject   *Error
	PayloadInvalidJSON *Error
	// StrictPayload (PayloadObject): the payload text (a JSON string is
	// unwrapped, any other value is taken as is, absent/null/"" is {}) must
	// pass jsonx.Strict (else PayloadInvalidJSON) and be an object (else
	// PayloadNotObject).
	StrictPayload bool
	// StructParams (PayloadObject) reads action and payload like a Go struct
	// (case-insensitive member names) instead of exact member names.
	StructParams bool
	// MaxResultBytes (0: unlimited) and ResultTooLarge bound invoke results;
	// results that are not valid JSON also get ResultTooLarge.
	MaxResultBytes int
	ResultTooLarge *Error
	// MaxReplyBytes (0: unlimited) bounds every reply line including its
	// newline; a longer reply is replaced by the ResultTooLarge error.
	MaxReplyBytes int
	// MapError converts non-*Error handler errors (default: -32000 err.Error()).
	MapError func(error) *Error
	// PanicError answers a handler panic (default -32000 "Internal error").
	PanicError *Error
	// Admit, when set, runs synchronously (under the loop lock, after every
	// other invoke check) and may reject the invoke with an exact error, e.g.
	// a plugin-owned busy gate that only some actions take. The plugin
	// releases whatever it acquired from its handler.
	Admit func(action string, payload json.RawMessage) *Error

	// --- tasks ---

	// Tasks enables plugin.task.* when the host negotiates features.tasks=1;
	// nil answers them with MethodNotFound.
	Tasks *TaskOptions
	// TasksNotNegotiated is the -32601 message for task methods before a
	// negotiating initialize (default "Tasks were not negotiated").
	TasksNotNegotiated string
	// RejectTaskMethods (Tasks == nil): answer every plugin.task.* method with
	// TasksNotNegotiated instead of MethodNotFound.
	RejectTaskMethods bool

	// --- lifecycle ---

	// DrainReplies: on shutdown and EOF, cancel in-flight work, wait up to
	// DrainTimeout and write their replies (then the shutdown reply). Without
	// it, the shutdown reply is written first and in-flight replies are
	// dropped.
	DrainReplies bool
	// DrainTimeout bounds waiting for in-flight work (default 1.5s).
	DrainTimeout time.Duration
	// ShutdownReplyLast (without DrainReplies): write the shutdown reply only
	// after in-flight work drained (their replies are still dropped).
	ShutdownReplyLast bool
	// CloseBeforeShutdownReply (with ShutdownReplyLast): run OnClose before
	// the shutdown reply, so disposal finished once the host sees it.
	CloseBeforeShutdownReply bool
	// WaitAll waits for every in-flight invoke and task to return, without
	// DrainTimeout, before OnClose runs and Serve returns (plugins that
	// released process-wide state only after all work finished).
	WaitAll bool
	// OnClose runs once after in-flight work drained (close sessions etc.).
	OnClose func()

	// Host configures the reverse-request client (Done is set by Serve).
	Host host.Options
	// HostReplyError, when set, decides whether a host reply is an error:
	// it receives the reply's raw "error" member (nil when absent) and
	// returns nil for a successful reply (the "result" member is then
	// delivered). Default: a present, non-null error decoded as
	// {code int, message string, data} (-32000 when it does not decode).
	HostReplyError func(raw json.RawMessage) *host.WireError
}

// TaskOptions configure plugin.task.* (runtime tasks feature).
type TaskOptions struct {
	// IDPattern validates task ids (default ^[a-zA-Z0-9_-]{1,64}$).
	IDPattern *regexp.Regexp
	// Limits (defaults 64 retained, 2 running, 300s deadline, 5m retention,
	// 256 KiB result, 4096 B progress, 1000 B error).
	MaxRetained      int
	MaxRunning       int
	Deadline         time.Duration
	Retention        time.Duration
	MaxResultBytes   int
	MaxProgressBytes int
	MaxErrorBytes    int
	// Messages (-32602 unless noted).
	NotFound        string // default "Task not found"
	InvalidID       string // default "Invalid or duplicate task ID"
	DuplicateID     string // default InvalidID
	LookupInvalidID string // status/cancel with a malformed id (default NotFound)
	LimitReached    *Error // default -32000 "Task count limit reached"
	Busy            *Error // default -32029 "Too many running tasks; try again later"
	// Snapshot error texts.
	Cancelled      string // default "Task cancelled"
	Timeout        string // default: the handler error message
	ResultTooLarge string // default "Task result exceeds limit"
	// Prepare, when set, runs synchronously at start; an error records the
	// task as failed immediately (its message becomes the snapshot error).
	Prepare func(action string, payload json.RawMessage) error
	// Admit, when set, runs after the id checks and before the retained and
	// running limits; a non-nil error is the start reply (no task recorded).
	Admit func(action string, payload json.RawMessage) *Error
	// Start, when set, replaces Prepare and Handler for tasks: it validates
	// the start synchronously (an error records a failed task like Prepare)
	// and returns the work to run. The TaskFunc context carries the host
	// caller and ReportProgress like a handler's.
	Start func(action string, payload json.RawMessage) (TaskFunc, error)
	// LookupBadParams answers status/cancel params that do not decode as
	// {taskId string} (default: ignored, the id is checked as given).
	LookupBadParams string
	// SharedGate: task starts are validated like invokes (busy gate shared
	// with invokes, then action, then payload) before the id is checked
	// (Node-family order).
	SharedGate bool
	// CheckPayload (without SharedGate, PayloadObject): the start payload may
	// be any JSON value; after the id checks the action is checked with
	// KnownAction and the payload normalised like an invoke's, before Admit
	// and the retained and running limits.
	CheckPayload bool
	// ClearProgressOnSuccess drops the last progress from a succeeded task.
	ClearProgressOnSuccess bool
	// SnapshotEscapeHTML marshals snapshots with json.Marshal HTML escaping.
	SnapshotEscapeHTML bool
	// SnapshotJS writes snapshots like JSON.stringify (U+2028/2029 stay
	// literal in strings); it takes precedence over SnapshotEscapeHTML.
	SnapshotJS bool
	// FlagOnlyCancel: plugin.task.cancel and the deadline only set
	// cancelRequested; the task context is not cancelled and a task never
	// ends "cancelled" (Node runtime behaviour).
	FlagOnlyCancel bool
	// ClipError, when set, turns a failed task's error message into the
	// snapshot error text instead of the MaxErrorBytes byte clip.
	ClipError func(message string) string
}

// TaskFunc is task work returned by TaskOptions.Start.
type TaskFunc func(ctx context.Context) (any, error)

// TaskResult, returned by a task handler or TaskFunc as its result with a nil
// error, sets the terminal snapshot verbatim instead of the default rules:
// State ("succeeded", "failed" or "cancelled"), Result (valid JSON or nil),
// Error, and CancelRequested (true sets cancelRequested; it is never cleared).
type TaskResult struct {
	State           string
	Result          json.RawMessage
	Error           string
	CancelRequested bool
}
