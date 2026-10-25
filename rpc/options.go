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
	CodeInvalidRequest = -32600
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

// LargeStorageLineBytes is the input line limit after features.largeStorage
// was negotiated: a host reply carrying a full 4 MiB storage value.
const LargeStorageLineBytes = 4<<20 + 64<<10

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
	// LargeStorage opts in to features.largeStorage: when the host offers it,
	// host.storage values up to 4 MiB can be read and written, and the input
	// limits (MaxLineBytes, a non-zero FatalLineBytes) rise to
	// LargeStorageLineBytes. Hosts without the feature keep every message,
	// and so every stored value, within 1 MiB.
	LargeStorage bool
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
	Invalid InvalidMode
	// JSONRPC2 completes JSON-RPC 2.0: a line holding an array is a batch
	// answered with one array (§6), and a parsable message that is not a valid
	// request gets -32600 echoing its id when valid, else null (§5.1).
	JSONRPC2       bool
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
	// OnInitialize, when set, runs once in its own goroutine after the first
	// plugin.initialize reply. The Caller stays usable until the transport
	// stops, so background work (a long-lived connection, a poller) can call
	// the host outside an invoke; ctx ends when Serve returns.
	OnInitialize func(ctx context.Context, h host.Caller)
	// OnConfigurationChanged, when set, negotiates features.configuration = 1
	// in plugin.initialize and runs in its own goroutine for each
	// plugin.configuration.changed request (after the empty reply). keys are
	// the settings whose effective value changed; re-read them with
	// host.GetConfiguration. Nil keeps the initialize result unchanged.
	OnConfigurationChanged func(ctx context.Context, keys []string, h host.Caller)

	// Events are host events (host.EventWorkspaceChanged, ...) subscribed right
	// after the first plugin.initialize; they need OnEvent. Events carrying
	// paths need the workspace.read capability.
	Events []string
	// OnEvent runs in its own goroutine for each plugin.event request, after
	// the empty reply. data is the event payload. Nil answers plugin.event
	// with method-not-found.
	OnEvent func(ctx context.Context, event string, data json.RawMessage, h host.Caller)

	// OnUIMessage receives free-form messages the plugin's views posted with
	// transport.postMessage (plugin.ui.message), in order on one goroutine;
	// viewID is the sending view's manifest ID. Reply with PostUIMessage. Nil
	// answers plugin.ui.message with method-not-found. Messages carry no risk
	// confirmation: keep writes and dangerous operations in actions.
	OnUIMessage func(ctx context.Context, viewID string, message json.RawMessage)

	// --- invoke ---

	// Handler runs plugin.invoke actions. It is required unless Database is
	// set; a database-driver-only plugin may omit it (every action then gets
	// UnknownAction).
	Handler Handler
	// MaxInflight bounds concurrent invokes (and tasks when
	// TaskOptions.SharedGate); excess requests get Busy (default 1: handlers
	// that were never audited for concurrency run one at a time).
	MaxInflight int
	Busy        *Error
	// ConcurrentActions lists actions whose handlers are safe to run at the
	// same time as any other invoke or task of the plugin: read-only, and
	// touching shared state only under their own synchronization. They bypass
	// the MaxInflight gate (also the SharedGate of tasks) and are bounded by
	// MaxConcurrent instead. Declaring one makes the action name known before
	// the busy check, so params are validated first for every invoke (like
	// ParamsBeforeBusy). Leave an action out unless its handler was audited.
	ConcurrentActions []string
	// MaxConcurrent bounds concurrently running ConcurrentActions (default 16);
	// excess requests get Busy.
	MaxConcurrent int
	// BusyWait, when positive, queues an invoke that finds the MaxInflight
	// gate full for up to this long (first come, first served) instead of
	// answering Busy at once; it gets Busy only when no slot frees in time or
	// MaxQueued invokes already wait. Every other admission check still runs
	// first. Task starts never wait. Keep it well below the host's 30 s invoke
	// timeout (for example 2 s).
	BusyWait time.Duration
	// MaxQueued bounds invokes waiting for a slot under BusyWait (default 16).
	MaxQueued int
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

	// --- plugin views, identity and transport (protocol_ext.go) ---

	// Methods are the plugin.call methods its views may call directly with
	// transport.request(method, params). They are view-only: no risk
	// confirmation and never offered to AI, so writes belong in actions.
	// params is the raw JSON value the view sent (null when omitted).
	Methods map[string]MethodFunc
	// Identity is reported in the plugin.initialize result so the host can
	// reject an entry that belongs to another plugin. When nil, the ids
	// injected at build time with -ldflags "-X
	// github.com/mincodeai/plugin-sdk-go/rpc.BuildPluginID=<id> -X
	// github.com/mincodeai/plugin-sdk-go/rpc.BuildPluginVersion=<version>"
	// are used; without either the result is unchanged.
	Identity *Identity
	// Transport must match the manifest's runtime.transport: "" or "jsonl"
	// (one JSON message per line) or "framed" (length-prefixed frames that
	// also carry binary data; see SendBinary).
	Transport string
	// OnBinary receives binary frames the plugin's views sent (framed
	// transport only). Frames are delivered in order on one goroutine.
	OnBinary func(ctx context.Context, channel string, data []byte)

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

	// --- database drivers ---

	// Database, when set, serves the database driver protocol v1
	// (plugin.db.*, docs/architecture/database-driver-protocol.md): it
	// negotiates features.databaseDrivers = 1 when the host offers it and adds
	// the top-level "databaseDrivers" member to the initialize result. Use
	// dbdriver.NewServer. Nil keeps the initialize result unchanged and
	// answers plugin.db.* with MethodNotFound.
	Database DatabaseHandler

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
	// DisableEvents stops negotiating features.taskEvents. By default, when
	// the host offers taskEvents = 1, Serve echoes it and sends a
	// plugin.task.changed notification ({"taskId": ...}) whenever a task
	// finishes or reports progress (progress at most every
	// TaskEventInterval), so the host refreshes the task at once instead of
	// waiting for its next status poll.
	DisableEvents bool
	// TaskEventInterval throttles progress notifications per task (default
	// 100ms); terminal states are always announced immediately.
	TaskEventInterval time.Duration
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

// DatabaseHandler serves the plugin.db.* methods of the database driver
// protocol (implemented by dbdriver.Server; the interface keeps this package
// free of database dependencies).
type DatabaseHandler interface {
	// DatabaseDetails is the compact JSON object written as the initialize
	// result's top-level "databaseDrivers" member ({"protocol":1,"drivers":[...]}).
	DatabaseDetails() json.RawMessage
	// StartDatabaseCall runs synchronously in the read loop: it validates the
	// params, applies admission limits and registers request ids (so a later
	// plugin.db.cancel always finds them), then returns the work that Serve
	// runs in its own goroutine. ctx ends when Serve stops. A non-nil *Error is
	// the reply; the work's error goes through the usual error mapping
	// (*Error verbatim).
	StartDatabaseCall(ctx context.Context, method string, params json.RawMessage) (func() (any, error), *Error)
	// CloseDatabase closes every handle and cursor; Serve calls it once when
	// the transport stops (plugin.shutdown, EOF), before OnClose.
	CloseDatabase()
}
