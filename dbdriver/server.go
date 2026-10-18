package dbdriver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/mincodeai/plugin-sdk-go/jsonx"
	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// Protocol limits (contract databaseDrivers limits).
const (
	// DefaultPageMaxBytes / DefaultPageMaxRows are the protocol 1.0 page
	// budget, used when connect sends no limits.
	DefaultPageMaxBytes = 524288
	DefaultPageMaxRows  = 1000
	// MaxPageBytes / MaxPageRows are the protocol 1.1 page ceiling the Server
	// announces in pageLimits; a 1.1 host sends at most these.
	MaxPageBytes      = 2 << 20
	MaxPageRows       = 5000
	DefaultCursorIdle = 60 * time.Second
	DefaultMaxCalls   = 16
	DefaultMaxHandles = 32
	MaxColumns        = 1024

	// lineBudget bounds any result that is not a 1.1 page so its reply line
	// stays under the 1 MiB runtime line cap (the envelope and id take the
	// rest). The host raises its cap to 4 MiB only for page replies.
	lineBudget = 1<<20 - 8<<10

	maxIDLength     = 128
	maxNoticeRunes  = 1024
	maxErrorRunes   = 4096
	maxVersionBytes = 128
	// maxPageTokenBytes is the host limit for tableRows nextPageToken.
	maxPageTokenBytes = 1024
)

// ServerOptions tune a Server. The zero value applies the protocol defaults.
type ServerOptions struct {
	// MaxCalls bounds concurrently running database calls (default 16);
	// excess calls fail with KindBusy. cancel, close and closeCursor are
	// never limited.
	MaxCalls int
	// MaxHandles bounds open connections (default 32).
	MaxHandles int
	// CancelWait is how long plugin.db.cancel waits for the cancelled call to
	// stop before answering "requested" instead of "confirmed" (default 2s,
	// below the host's 5 s grace).
	CancelWait time.Duration
	// CloseWait bounds waiting for running calls when a handle closes or the
	// transport stops (default 3s).
	CloseWait time.Duration
	// MetadataTimeout bounds metadata calls (default 120s) and WriteTimeout
	// mutate/destructive calls (default 600s); the host sends no timeout for
	// them.
	MetadataTimeout time.Duration
	WriteTimeout    time.Duration
	// OversizedRowNotice is the notice that ends a result whose next row
	// alone exceeds the page budget (row is 1-based).
	OversizedRowNotice func(row, limitBytes int) string
	// ProtocolV10 announces protocol 1.0 only (no minor, no pageLimits): the
	// host keeps 1000-row / 512 KiB pages and the 1 MiB line cap. Set it when
	// rpc.Options.MaxReplyBytes is below MaxPageBytes plus the envelope,
	// otherwise large pages would be replaced by ResultTooLarge.
	ProtocolV10 bool
	// DisablePrefetch turns off plugin-side read-ahead: by default, after
	// answering a page the Server reads and encodes the cursor's next page
	// while the host processes this one (at most one page per cursor).
	DisablePrefetch bool
	// ColumnarEncoding (experimental, protocol v2 draft) announces the
	// columnar page encoding (EncodingColumnar) in the initialize details.
	// A host that enables it too requests it per connection in
	// plugin.db.connect; only then are that handle's execute, tableRows and
	// fetch pages sent as base64 column blocks instead of JSON rows. Hosts
	// that do not request it, and protocol 1.0 (ProtocolV10), keep JSON rows.
	ColumnarEncoding bool
}

func (o *ServerOptions) setDefaults() {
	if o.MaxCalls <= 0 {
		o.MaxCalls = DefaultMaxCalls
	}
	if o.MaxHandles <= 0 {
		o.MaxHandles = DefaultMaxHandles
	}
	if o.CancelWait <= 0 {
		o.CancelWait = 2 * time.Second
	}
	if o.CloseWait <= 0 {
		o.CloseWait = 3 * time.Second
	}
	if o.MetadataTimeout <= 0 {
		o.MetadataTimeout = 120 * time.Second
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 600 * time.Second
	}
	if o.OversizedRowNotice == nil {
		o.OversizedRowNotice = func(row, limit int) string {
			return "查询结果已截断：第 " + strconv.Itoa(row) + " 行超过单页 " + strconv.Itoa(limit/1024) + " KiB 上限 / Result truncated: row " + strconv.Itoa(row) + " exceeds the page limit"
		}
	}
}

// Server serves plugin.db.* for a set of drivers. Set it as
// rpc.Options.Database; it implements rpc.DatabaseHandler.
type Server struct {
	drivers map[string]Driver
	ids     []string
	o       ServerOptions

	mu         sync.Mutex
	handles    map[string]*handle
	pending    int // connects in flight (count against MaxHandles)
	calls      int
	nextHandle uint64
	nextCursor uint64
	closed     bool
}

var _ rpc.DatabaseHandler = (*Server)(nil)

// NewServer returns a Server for drivers keyed by the driver ids the manifest
// contributes (contributes.databaseDrivers[].id).
func NewServer(drivers map[string]Driver, o ServerOptions) *Server {
	o.setDefaults()
	s := &Server{drivers: map[string]Driver{}, o: o, handles: map[string]*handle{}}
	for id, d := range drivers {
		if d == nil {
			continue
		}
		s.drivers[id] = d
		s.ids = append(s.ids, id)
	}
	sort.Strings(s.ids)
	return s
}

// DriverIDs returns the served driver ids, sorted.
func (s *Server) DriverIDs() []string { return append([]string(nil), s.ids...) }

// DatabaseDetails implements rpc.DatabaseHandler.
func (s *Server) DatabaseDetails() json.RawMessage {
	ids := s.ids
	if ids == nil {
		ids = []string{}
	}
	if s.o.ProtocolV10 {
		raw, _ := json.Marshal(struct {
			Protocol int      `json:"protocol"`
			Drivers  []string `json:"drivers"`
		}{Protocol, ids})
		return raw
	}
	type pageLimits struct {
		MaxRows  int `json:"maxRows"`
		MaxBytes int `json:"maxBytes"`
	}
	var encodings []string
	if s.o.ColumnarEncoding {
		encodings = []string{EncodingColumnar}
	}
	raw, _ := json.Marshal(struct {
		Protocol   int        `json:"protocol"`
		Minor      int        `json:"minor"`
		Drivers    []string   `json:"drivers"`
		PageLimits pageLimits `json:"pageLimits"`
		Encodings  []string   `json:"encodings,omitempty"`
	}{Protocol, ProtocolMinor, ids, pageLimits{MaxPageRows, MaxPageBytes}, encodings})
	return raw
}

type limits struct {
	pageMaxBytes int
	pageMaxRows  int
	cursorIdle   time.Duration
}

type handle struct {
	id      string
	conn    Conn
	info    ConnInfo
	limits  limits
	secrets []string
	// columnar is set when connect negotiated EncodingColumnar.
	columnar bool

	ctx    context.Context
	cancel context.CancelCauseFunc
	// running counts calls in flight on this handle; idle is closed (and
	// replaced) whenever it drops to zero.
	running  int
	idle     chan struct{}
	requests map[string]*request
	cursors  map[string]*cursor
	closed   bool
}

// request is a cancellable requestId (execute, tableRows, mutate,
// destructive). It lives until its call ended and its cursor closed.
type request struct {
	id     string
	ctx    context.Context
	cancel context.CancelFunc
	// busy is true while a driver call for the request runs; done is closed
	// when it stops.
	busy   bool
	done   chan struct{}
	cursor *cursor
}

// aheadRow is a row read but not yet sent: enc is its JSON encoding, or for
// a columnar cursor its tagged cells (appendColumnarRow), whose length is
// the row's columnar size bound; size is len(enc). num is its 1-based
// position in the result (for the oversized-row notice); 0 means no row.
type aheadRow struct {
	enc  []byte
	size int
	num  int
}

func (r aheadRow) ok() bool { return r.num > 0 }

type cursor struct {
	id  string
	h   *handle
	req *request // nil for metadata cursors
	// rows is the open result (nil for metadata cursors).
	rows    Rows
	columns int
	// columnar pages are sent in EncodingColumnar (execute and tableRows
	// cursors of a handle that negotiated it; never backup streams).
	columnar bool
	peek     aheadRow // the row that did not fit the last page (enc nil: none)
	rowNum   int      // rows read from rows so far
	notice   string   // pending notice for the next page
	// ahead holds rows prefetched while the host processed the last page
	// (at most that page's budget); aheadErr / aheadEOF end the result after
	// them. While prefetching is true only the prefetch goroutine touches
	// rows, peek, ahead, rowNum, aheadErr and aheadEOF; prefetchDone is
	// closed when it stops.
	ahead        []aheadRow
	aheadBytes   int
	aheadErr     error
	aheadEOF     bool
	prefetching  bool
	prefetchDone chan struct{}
	// items are the remaining encoded metadata items.
	items  [][]byte
	busy   bool
	closed bool
	// closeWhenIdle closes the cursor when its running call returns
	// (closeCursor during a fetch).
	closeWhenIdle bool
	timer         *time.Timer
}

func (s *Server) scrubber(secrets []string) func(string) string {
	return func(msg string) string {
		for _, v := range secrets {
			if v != "" {
				msg = strings.ReplaceAll(msg, v, Redacted)
			}
		}
		return msg
	}
}

// StartDatabaseCall implements rpc.DatabaseHandler.
func (s *Server) StartDatabaseCall(ctx context.Context, method string, params json.RawMessage) (func() (any, error), *rpc.Error) {
	switch method {
	case "plugin.db.connect":
		return s.startConnect(ctx, params)
	case "plugin.db.execute":
		return s.startExecute(params)
	case "plugin.db.fetch":
		return s.startFetch(params)
	case "plugin.db.closeCursor":
		return s.startCloseCursor(params)
	case "plugin.db.cancel":
		return s.startCancel(params)
	case "plugin.db.metadata":
		return s.startMetadata(params)
	case "plugin.db.backup":
		return s.startBackup(params)
	case "plugin.db.tableRows":
		return s.startTableRows(params)
	case "plugin.db.mutate":
		return s.startMutate(params)
	case "plugin.db.destructive":
		return s.startDestructive(params)
	case "plugin.db.close":
		return s.startClose(params)
	}
	return nil, rpc.NewError(rpc.CodeMethodNotFound, "Method not found")
}

// decodeParams decodes strict params (unknown members rejected). Messages
// name the field only, never a value.
func decodeParams(params json.RawMessage, v any) *rpc.Error {
	if len(params) == 0 {
		return invalidf("invalid params: params must be an object")
	}
	err := jsonx.DecodeObject(params, v, jsonx.DecodeOptions{Strict: true, DisallowUnknown: true, UseNumber: true})
	if err == nil {
		return nil
	}
	de, _ := err.(*jsonx.DecodeError)
	if de == nil {
		return invalidf("invalid params")
	}
	switch de.Kind {
	case jsonx.UnknownField:
		return invalidf("invalid params: unknown member %s", quote(de.Field))
	case jsonx.WrongType:
		return invalidf("invalid params: member %s has the wrong type", quote(de.Field))
	case jsonx.NotObject:
		return invalidf("invalid params: params must be an object")
	}
	return invalidf("invalid params")
}

func validID(id string) bool { return id != "" && len(id) <= maxIDLength }

func clipRunes(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	n := 0
	for i := range s {
		if n == max {
			return s[:i]
		}
		n++
	}
	return s
}

// validPageToken applies the host's nextPageToken rules.
func validPageToken(t string) bool {
	if len(t) > maxPageTokenBytes || !utf8.ValidString(t) {
		return false
	}
	for _, r := range t {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) {
			return false
		}
	}
	return true
}

func clipBytes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	for max > 0 && !utf8.RuneStart(s[max]) {
		max--
	}
	return s[:max]
}

// admitLocked applies the call gate and marks a call running on h.
func (s *Server) admitLocked(h *handle) *rpc.Error {
	if s.calls >= s.o.MaxCalls {
		return rpcError(Errorf(KindBusy, "too many database calls in flight; try again later"), nil)
	}
	s.calls++
	if h != nil {
		h.running++
	}
	return nil
}

// release ends a call admitted by admitLocked.
func (s *Server) release(h *handle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseLocked(h)
}

func (s *Server) releaseLocked(h *handle) {
	s.calls--
	if h != nil {
		h.running--
		if h.running == 0 {
			close(h.idle)
			h.idle = make(chan struct{})
		}
	}
}

func (s *Server) handleLocked(id string) (*handle, *rpc.Error) {
	if s.closed {
		return nil, rpcError(Errorf(KindConnection, "driver is shutting down"), nil)
	}
	h := s.handles[id]
	if h == nil || h.closed {
		return nil, rpcError(Errorf(KindConnection, "unknown or closed handle"), nil)
	}
	return h, nil
}

// --- connect ---

type connectConfig struct {
	Host       string         `json:"host"`
	Port       *int           `json:"port"`
	Database   string         `json:"database"`
	Username   string         `json:"username"`
	Password   Secret         `json:"password"`
	Token      Secret         `json:"token"`
	FilePath   string         `json:"filePath"`
	SSL        *bool          `json:"ssl"`
	Org        string         `json:"org"`
	AuthSource string         `json:"authSource"`
	Options    map[string]any `json:"options"`
}

type connectParams struct {
	DriverID string        `json:"driverId"`
	Config   connectConfig `json:"config"`
	Limits   struct {
		PageMaxBytes int `json:"pageMaxBytes"`
		PageMaxRows  int `json:"pageMaxRows"`
		CursorIdleMs int `json:"cursorIdleMs"`
	} `json:"limits"`
	TimeoutMs int `json:"timeoutMs"`
	// Encoding requests an experimental page encoding (only sent by hosts
	// that saw it announced in the initialize details).
	Encoding string `json:"encoding"`
}

func (s *Server) startConnect(base context.Context, params json.RawMessage) (func() (any, error), *rpc.Error) {
	var p connectParams
	if e := decodeParams(params, &p); e != nil {
		return nil, e
	}
	d := s.drivers[p.DriverID]
	if d == nil {
		return nil, rpcError(Errorf(KindUnsupported, "unknown database driver %s", quote(clipBytes(p.DriverID, 64))), nil)
	}
	if p.Config.SSL == nil {
		return nil, invalidf("invalid params: config.ssl is required")
	}
	if p.Config.Port != nil && (*p.Config.Port < 0 || *p.Config.Port > 65535) {
		return nil, invalidf("invalid params: config.port is out of range")
	}
	if p.Limits.PageMaxBytes < 0 || p.Limits.PageMaxRows < 0 || p.Limits.CursorIdleMs < 0 || p.TimeoutMs < 0 {
		return nil, invalidf("invalid params: limits must not be negative")
	}
	columnar := false
	switch p.Encoding {
	case "":
	case EncodingColumnar:
		if !s.o.ColumnarEncoding || s.o.ProtocolV10 {
			return nil, invalidf("invalid params: encoding %s was not announced", EncodingColumnar)
		}
		columnar = true
	default:
		return nil, invalidf("invalid params: unknown encoding")
	}
	lim := limits{pageMaxBytes: DefaultPageMaxBytes, pageMaxRows: DefaultPageMaxRows, cursorIdle: DefaultCursorIdle}
	maxBytes, maxRows := MaxPageBytes, MaxPageRows
	if s.o.ProtocolV10 {
		// A 1.0 host never raises its 1 MiB line cap.
		maxBytes, maxRows = lineBudget, p.Limits.PageMaxRows
	}
	if p.Limits.PageMaxBytes > 0 {
		lim.pageMaxBytes = min(p.Limits.PageMaxBytes, maxBytes)
	}
	if p.Limits.PageMaxRows > 0 {
		lim.pageMaxRows = min(p.Limits.PageMaxRows, maxRows)
	}
	if p.Limits.CursorIdleMs > 0 {
		lim.cursorIdle = time.Duration(p.Limits.CursorIdleMs) * time.Millisecond
	}
	cfg := Config{
		Host: p.Config.Host, Database: p.Config.Database, Username: p.Config.Username,
		Password: p.Config.Password, Token: p.Config.Token, FilePath: p.Config.FilePath,
		SSL: *p.Config.SSL, Org: p.Config.Org, AuthSource: p.Config.AuthSource, Options: p.Config.Options,
	}
	if p.Config.Port != nil {
		cfg.Port = *p.Config.Port
	}
	if cfg.Options == nil {
		cfg.Options = map[string]any{}
	}
	secrets := []string{cfg.Password.Reveal(), cfg.Token.Reveal()}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, rpcError(Errorf(KindConnection, "driver is shutting down"), nil)
	}
	if len(s.handles)+s.pending >= s.o.MaxHandles {
		s.mu.Unlock()
		return nil, rpcError(Errorf(KindBusy, "too many open connections"), nil)
	}
	if e := s.admitLocked(nil); e != nil {
		s.mu.Unlock()
		return nil, e
	}
	s.pending++
	s.mu.Unlock()
	timeout := time.Duration(p.TimeoutMs) * time.Millisecond
	return func() (any, error) {
		defer func() {
			s.mu.Lock()
			s.pending--
			s.releaseLocked(nil)
			s.mu.Unlock()
		}()
		scrub := s.scrubber(secrets)
		cctx, cancel := base, context.CancelFunc(func() {})
		if timeout > 0 {
			cctx, cancel = context.WithTimeout(base, timeout)
		}
		conn, info, err := d.Connect(cctx, cfg)
		ctxErr := cctx.Err()
		cancel()
		if err != nil {
			if ctxErr == nil {
				return nil, rpcError(classify(nil, err, KindConnection), scrub)
			}
			return nil, rpcError(classify(cctx, err, KindConnection), scrub)
		}
		if conn == nil {
			return nil, rpcError(Errorf(KindConnection, "driver returned no connection"), nil)
		}
		hctx, hcancel := context.WithCancelCause(base)
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			hcancel(errHandleClosed)
			_ = conn.Close()
			return nil, rpcError(Errorf(KindConnection, "driver is shutting down"), nil)
		}
		s.nextHandle++
		h := &handle{
			id: "h" + strconv.FormatUint(s.nextHandle, 10), conn: conn, info: info, limits: lim, secrets: secrets, columnar: columnar,
			ctx: hctx, cancel: hcancel, idle: make(chan struct{}),
			requests: map[string]*request{}, cursors: map[string]*cursor{},
		}
		s.handles[h.id] = h
		s.mu.Unlock()
		return connectResult{Handle: h.id, Info: connectInfo{
			ServerVersion:   clipBytes(info.ServerVersion, maxVersionBytes),
			DefaultDatabase: info.DefaultDatabase,
			ReadOnly:        info.ReadOnly,
			BackupVersion:   backupVersionOf(conn),
			Encoding:        encodingOf(columnar),
		}}, nil
	}, nil
}

// backupVersionOf announces plugin.db.backup only for a Conn that
// implements BackupProvider.
func backupVersionOf(conn Conn) int {
	if _, ok := conn.(BackupProvider); ok {
		return BackupVersion
	}
	return 0
}

func encodingOf(columnar bool) string {
	if columnar {
		return EncodingColumnar
	}
	return ""
}

type connectInfo struct {
	ServerVersion   string `json:"serverVersion,omitempty"`
	DefaultDatabase string `json:"defaultDatabase,omitempty"`
	ReadOnly        bool   `json:"readOnly"`
	BackupVersion   int    `json:"backupVersion,omitempty"`
	// Encoding confirms the page encoding requested in connect.
	Encoding string `json:"encoding,omitempty"`
}

type connectResult struct {
	Handle string      `json:"handle"`
	Info   connectInfo `json:"info"`
}

// --- execute / tableRows ---

type executeParams struct {
	Handle    string `json:"handle"`
	RequestID string `json:"requestId"`
	Statement string `json:"statement"`
	Database  string `json:"database"`
	Mode      Mode   `json:"mode"`
	ReadOnly  *bool  `json:"readOnly"`
	MaxRows   int    `json:"maxRows"`
	MaxBytes  int    `json:"maxBytes"`
	TimeoutMs int    `json:"timeoutMs"`
}

type pageBudget struct{ rows, bytes int }

func (h *handle) budget(maxRows, maxBytes int) (pageBudget, *rpc.Error) {
	if maxRows <= 0 || maxBytes <= 0 {
		return pageBudget{}, invalidf("invalid params: maxRows and maxBytes must be positive")
	}
	// h.limits is already clamped to the negotiated ceiling (connect).
	return pageBudget{rows: min(maxRows, h.limits.pageMaxRows), bytes: min(maxBytes, h.limits.pageMaxBytes)}, nil
}

// beginRequestLocked registers a cancellable request on h.
func (s *Server) beginRequestLocked(h *handle, id string, timeout time.Duration) (*request, *rpc.Error) {
	if !validID(id) {
		return nil, invalidf("invalid params: requestId must be 1-%d characters", maxIDLength)
	}
	if h.requests[id] != nil {
		return nil, invalidf("invalid params: requestId is already running")
	}
	if e := s.admitLocked(h); e != nil {
		return nil, e
	}
	ctx, cancel := h.ctx, context.CancelFunc(nil)
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(h.ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(h.ctx)
	}
	r := &request{id: id, ctx: ctx, cancel: cancel, busy: true, done: make(chan struct{})}
	h.requests[id] = r
	return r, nil
}

// endCallLocked marks the request's driver call finished. Without an open
// cursor the request is removed and its context released.
func (s *Server) endCallLocked(h *handle, r *request) {
	if r.busy {
		r.busy = false
		close(r.done)
	}
	if r.cursor == nil || r.cursor.closed {
		r.cancel()
		if h.requests[r.id] == r {
			delete(h.requests, r.id)
		}
	}
	s.releaseLocked(h)
}

func (s *Server) startExecute(params json.RawMessage) (func() (any, error), *rpc.Error) {
	var p executeParams
	if e := decodeParams(params, &p); e != nil {
		return nil, e
	}
	switch p.Mode {
	case ModeAuto, ModeQuery, ModeExec:
	default:
		return nil, invalidf("invalid params: mode must be auto, query or exec")
	}
	if p.Statement == "" || p.ReadOnly == nil || p.TimeoutMs < 0 {
		return nil, invalidf("invalid params: statement and readOnly are required")
	}
	req := ExecuteRequest{RequestID: p.RequestID, Statement: p.Statement, Database: p.Database, Mode: p.Mode, ReadOnly: *p.ReadOnly}
	return s.startResult(p.Handle, p.RequestID, p.MaxRows, p.MaxBytes, time.Duration(p.TimeoutMs)*time.Millisecond,
		func(ctx context.Context, conn Conn) (*Result, error) { return conn.Execute(ctx, req) }, false, true)
}

type tableRowsParams struct {
	Handle    string `json:"handle"`
	RequestID string `json:"requestId"`
	Database  string `json:"database"`
	Table     string `json:"table"`
	Limit     int    `json:"limit"`
	Offset    int    `json:"offset"`
	PageToken string `json:"pageToken"`
	ReadOnly  *bool  `json:"readOnly"`
	MaxRows   int    `json:"maxRows"`
	MaxBytes  int    `json:"maxBytes"`
}

func (s *Server) startTableRows(params json.RawMessage) (func() (any, error), *rpc.Error) {
	var p tableRowsParams
	if e := decodeParams(params, &p); e != nil {
		return nil, e
	}
	if p.ReadOnly == nil || !*p.ReadOnly {
		return nil, invalidf("invalid params: tableRows requires readOnly true")
	}
	if p.Table == "" || p.Limit < 0 || p.Offset < 0 {
		return nil, invalidf("invalid params: table is required and limit/offset must not be negative")
	}
	req := TableRowsRequest{RequestID: p.RequestID, Database: p.Database, Table: p.Table, Limit: p.Limit, Offset: p.Offset, PageToken: p.PageToken}
	return s.startResult(p.Handle, p.RequestID, p.MaxRows, p.MaxBytes, s.o.MetadataTimeout,
		func(ctx context.Context, conn Conn) (*Result, error) {
			tp, ok := conn.(TableRowsProvider)
			if !ok {
				return nil, ErrUnsupported
			}
			return tp.TableRows(ctx, req)
		}, true, true)
}

// startResult runs a call producing a Result and answers its first page.
// columnar allows the negotiated columnar encoding for the result's pages.
func (s *Server) startResult(handleID, requestID string, maxRows, maxBytes int, timeout time.Duration,
	call func(context.Context, Conn) (*Result, error), tableRows, columnar bool) (func() (any, error), *rpc.Error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, e := s.handleLocked(handleID)
	if e != nil {
		return nil, e
	}
	b, e := h.budget(maxRows, maxBytes)
	if e != nil {
		return nil, e
	}
	r, e := s.beginRequestLocked(h, requestID, timeout)
	if e != nil {
		return nil, e
	}
	return func() (out any, err error) {
		scrub := s.scrubber(h.secrets)
		var c *cursor
		defer func() {
			s.mu.Lock()
			if c != nil && !c.closed {
				c.busy = false
				s.startPrefetchLocked(c, b)
				s.armLocked(c)
			}
			s.endCallLocked(h, r)
			s.mu.Unlock()
		}()
		res, cerr := call(r.ctx, h.conn)
		if cerr != nil {
			return nil, rpcError(classify(r.ctx, cerr, KindDatabase), scrub)
		}
		if res == nil {
			res = &Result{}
		}
		notice := clipRunes(res.Notice, maxNoticeRunes)
		token := ""
		if tableRows {
			if token = res.NextPageToken; !validPageToken(token) {
				if res.Rows != nil {
					_ = res.Rows.Close()
				}
				return nil, rpcError(Errorf(KindDatabase, "driver returned an invalid nextPageToken (at most %d bytes of printable UTF-8)", maxPageTokenBytes), nil)
			}
		}
		if res.Rows == nil {
			return encodeHead(nil, res.AffectedRows, notice, token, "", true, pagePayload{}), nil
		}
		cols := res.Rows.Columns()
		if len(cols) > MaxColumns {
			_ = res.Rows.Close()
			return nil, rpcError(Errorf(KindDatabase, "result has more than %d columns", MaxColumns), nil)
		}
		if cols == nil {
			cols = []Column{}
		}
		s.mu.Lock()
		s.nextCursor++
		c = &cursor{id: "c" + strconv.FormatUint(s.nextCursor, 10), h: h, req: r, rows: res.Rows, columns: len(cols), columnar: columnar && h.columnar, busy: true, notice: notice}
		r.cursor = c
		h.cursors[c.id] = c
		s.mu.Unlock()
		page, perr := s.page(r.ctx, c, b, func(rows pagePayload, pageNotice, cursorID string, done bool) []byte {
			return encodeHead(cols, res.AffectedRows, pageNotice, token, cursorID, done, rows)
		})
		if perr != nil {
			s.closeCursor(c)
			return nil, rpcError(perr, scrub)
		}
		return page, nil
	}, nil
}

// pagePayload is the rows of one page: the comma-joined JSON rows, or for a
// columnar cursor the base64 column blocks.
type pagePayload struct {
	columnar bool
	rows     []byte
}

// appendRows appends the page's rows member(s).
func (p pagePayload) appendRows(b []byte) []byte {
	if p.columnar {
		b = append(b, `"encoding":"`+EncodingColumnar+`","data":"`...)
		b = append(b, p.rows...)
		return append(b, '"')
	}
	b = append(b, `"rows":[`...)
	b = append(b, p.rows...)
	return append(b, ']')
}

// encodeHead renders an execute-shaped result. rows is empty for a
// statement without a result set.
func encodeHead(cols []Column, affected *int64, notice, token, cursorID string, done bool, rows pagePayload) json.RawMessage {
	b := make([]byte, 0, len(rows.rows)+256)
	b = append(b, '{')
	if cols != nil {
		b = append(b, `"columns":[`...)
		for i, c := range cols {
			if i > 0 {
				b = append(b, ',')
			}
			b = append(b, `{"name":`...)
			b = appendJSONString(b, c.Name)
			if c.Type != "" {
				b = append(b, `,"type":`...)
				b = appendJSONString(b, c.Type)
			}
			b = append(b, '}')
		}
		b = append(b, "],"...)
	}
	b = rows.appendRows(b)
	if affected != nil {
		b = append(b, `,"affectedRows":`...)
		b = strconv.AppendInt(b, *affected, 10)
	}
	return encodeTail(b, notice, token, cursorID, done)
}

func encodeTail(b []byte, notice, token, cursorID string, done bool) json.RawMessage {
	if notice != "" {
		b = append(b, `,"notice":`...)
		b = appendJSONString(b, notice)
	}
	if token != "" {
		b = append(b, `,"nextPageToken":`...)
		b = appendJSONString(b, token)
	}
	if cursorID != "" && !done {
		b = append(b, `,"cursor":`...)
		b = appendJSONString(b, cursorID)
	}
	if done {
		b = append(b, `,"done":true}`...)
	} else {
		b = append(b, `,"done":false}`...)
	}
	return b
}

// page reads the next page of c within b. render builds the result from the
// page's rows; it is called once to measure the fixed part (with the
// worst-case notice and a cursor) and once for the result. The cursor is
// closed when the result is done. JSON rows count their exact encoded size;
// columnar rows count their size bound, base64-expanded, so the data member
// never exceeds the budget either.
func (s *Server) page(ctx context.Context, c *cursor, b pageBudget,
	render func(rows pagePayload, notice, cursorID string, done bool) []byte) (json.RawMessage, *Error) {
	oversized := s.o.OversizedRowNotice(c.rowNum+1+maxIDLength, b.bytes)
	worstNotice := c.notice
	if worstNotice != "" {
		worstNotice += " "
	}
	worstNotice = clipRunes(worstNotice+oversized, maxNoticeRunes)
	fixed := len(render(pagePayload{columnar: c.columnar}, worstNotice, c.id, false))
	if fixed > b.bytes {
		return nil, Errorf(KindDatabase, "result columns exceed the page size limit")
	}
	var rows []byte
	var cells [][]byte
	// used is the encoded size of the rows so far: JSON bytes, or the
	// columnar binary bound (header and column overhead included).
	used := 0
	if c.columnar {
		used = columnarPageBound(c.columns, 0)
	}
	n, done := 0, false
	notice := c.notice
	c.notice = ""
	for n < b.rows {
		row, eof, err := s.nextRow(ctx, c)
		if err != nil {
			return nil, classify(ctx, err, KindDatabase)
		}
		if eof {
			done = true
			break
		}
		var total int
		if c.columnar {
			total = fixed + base64.StdEncoding.EncodedLen(used+row.size)
		} else {
			add := row.size
			if n > 0 {
				add++
			}
			total = fixed + used + add
		}
		if total > b.bytes {
			if n == 0 {
				if notice != "" {
					notice += " "
				}
				notice = clipRunes(notice+s.o.OversizedRowNotice(row.num, b.bytes), maxNoticeRunes)
				done = true
				break
			}
			c.peek = row
			break
		}
		if c.columnar {
			cells = append(cells, row.enc)
		} else {
			if n > 0 {
				rows = append(rows, ',')
				used++
			}
			rows = append(rows, row.enc...)
		}
		used += row.size
		n++
	}
	if !done && !c.peek.ok() {
		// Read ahead so the last page says done.
		row, eof, err := s.nextRow(ctx, c)
		if err != nil {
			return nil, classify(ctx, err, KindDatabase)
		}
		if eof {
			done = true
		} else {
			c.peek = row
		}
	}
	payload := pagePayload{columnar: c.columnar, rows: rows}
	if c.columnar {
		bin := encodeColumnar(cells, c.columns, used)
		payload.rows = make([]byte, base64.StdEncoding.EncodedLen(len(bin)))
		base64.StdEncoding.Encode(payload.rows, bin)
	}
	out := render(payload, notice, c.id, done)
	if done {
		s.closeCursor(c)
	}
	return out, nil
}

// nextRow returns the next encoded row: the one that did not fit the last
// page, then prefetched rows, then the prefetch's end (error or EOF), then
// the driver.
func (s *Server) nextRow(ctx context.Context, c *cursor) (aheadRow, bool, error) {
	if c.peek.ok() {
		row := c.peek
		c.peek = aheadRow{}
		return row, false, nil
	}
	if len(c.ahead) > 0 {
		row := c.ahead[0]
		c.ahead[0] = aheadRow{}
		c.ahead = c.ahead[1:]
		c.aheadBytes -= row.size
		if len(c.ahead) == 0 {
			c.ahead, c.aheadBytes = nil, 0
		}
		return row, false, nil
	}
	if c.aheadErr != nil {
		err := c.aheadErr
		c.aheadErr = nil
		return aheadRow{}, false, err
	}
	if c.aheadEOF {
		return aheadRow{}, true, nil
	}
	return s.readRow(ctx, c)
}

// readRow reads and encodes one row from the driver.
func (s *Server) readRow(ctx context.Context, c *cursor) (aheadRow, bool, error) {
	row, err := c.rows.Next(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return aheadRow{}, true, nil
		}
		return aheadRow{}, false, err
	}
	c.rowNum++
	if len(row) != c.columns {
		return aheadRow{}, false, Errorf(KindDatabase, "driver returned %d values for %d columns", len(row), c.columns)
	}
	if c.columnar {
		enc := appendColumnarRow(make([]byte, 0, 64), row)
		return aheadRow{enc: enc, size: len(enc), num: c.rowNum}, false, nil
	}
	enc := make([]byte, 0, 64)
	enc = append(enc, '[')
	for i, v := range row {
		if i > 0 {
			enc = append(enc, ',')
		}
		enc = appendCell(enc, v)
	}
	enc = append(enc, ']')
	return aheadRow{enc: enc, size: len(enc), num: c.rowNum}, false, nil
}

// startPrefetchLocked starts reading c's next page (within b, the budget of
// the page just answered) while the host processes this one. The host keeps
// one fetch per cursor in flight and the prefetch stops at one page (plus the
// row that tells whether it is the last), so at most one page is buffered.
// The prefetch counts as running on the handle (closeHandle waits for it)
// but not against MaxCalls.
func (s *Server) startPrefetchLocked(c *cursor, b pageBudget) {
	h, r := c.h, c.req
	if s.o.DisablePrefetch || s.closed || h.closed || c.closed || c.busy || c.closeWhenIdle || c.prefetching ||
		c.rows == nil || r == nil || r.ctx.Err() != nil || c.aheadErr != nil || c.aheadEOF {
		return
	}
	wantRows, wantBytes := b.rows+1, b.bytes
	haveRows, haveBytes := len(c.ahead), c.aheadBytes
	if c.peek.ok() {
		haveRows++
		haveBytes += c.peek.size
	}
	if haveRows >= wantRows || haveBytes >= wantBytes {
		return
	}
	done := make(chan struct{})
	c.prefetching, c.prefetchDone = true, done
	h.running++
	go s.prefetch(r.ctx, c, wantRows-haveRows, wantBytes-haveBytes, done)
}

// prefetch reads up to wantRows rows / wantBytes encoded bytes into c.ahead.
// A driver error or the end of the result is kept for the next page. When the
// cursor was closed meanwhile it discards the rows and closes the result.
func (s *Server) prefetch(ctx context.Context, c *cursor, wantRows, wantBytes int, done chan struct{}) {
	n, size := 0, 0
	for n < wantRows && size < wantBytes {
		if err := ctx.Err(); err != nil {
			c.aheadErr = err
			break
		}
		row, eof, err := s.readRow(ctx, c)
		if err != nil {
			c.aheadErr = err
			break
		}
		if eof {
			c.aheadEOF = true
			break
		}
		c.ahead = append(c.ahead, row)
		c.aheadBytes += row.size
		n++
		size += row.size + 1
	}
	s.mu.Lock()
	c.prefetching = false
	close(done)
	var rows Rows
	if c.closed {
		rows = c.rows
		c.rows, c.peek, c.ahead, c.aheadBytes, c.aheadErr = nil, aheadRow{}, nil, 0, nil
	} else {
		s.armLocked(c)
	}
	h := c.h
	h.running--
	if h.running == 0 {
		close(h.idle)
		h.idle = make(chan struct{})
	}
	s.mu.Unlock()
	if rows != nil {
		_ = rows.Close()
	}
}

// closeCursor closes c once (rows closed outside the lock).
func (s *Server) closeCursor(c *cursor) {
	s.mu.Lock()
	rows := s.detachCursorLocked(c)
	s.mu.Unlock()
	if rows != nil {
		_ = rows.Close()
	}
}

// detachCursorLocked removes c and returns its rows for closing.
func (s *Server) detachCursorLocked(c *cursor) Rows {
	if c.closed {
		return nil
	}
	c.closed = true
	if c.timer != nil {
		c.timer.Stop()
	}
	if c.h.cursors[c.id] == c {
		delete(c.h.cursors, c.id)
	}
	if r := c.req; r != nil && !r.busy {
		r.cancel()
		if c.h.requests[r.id] == r {
			delete(c.h.requests, r.id)
		}
	}
	if c.prefetching {
		// The prefetch goroutine owns rows until it stops. Its request context
		// is cancelled here, so it stops promptly and closes rows itself.
		if c.req != nil {
			c.req.cancel()
		}
		c.items = nil
		return nil
	}
	rows := c.rows
	c.rows, c.peek, c.items, c.ahead, c.aheadBytes, c.aheadErr = nil, aheadRow{}, nil, nil, 0, nil
	return rows
}

// armLocked (re)starts the idle timer of an open, idle cursor.
func (s *Server) armLocked(c *cursor) {
	if c.closed || c.busy {
		return
	}
	if c.prefetching {
		// The prefetch re-arms the cursor when it stops.
		if c.timer != nil {
			c.timer.Stop()
		}
		return
	}
	if c.closeWhenIdle {
		if rows := s.detachCursorLocked(c); rows != nil {
			go func() { _ = rows.Close() }()
		}
		return
	}
	if c.timer != nil {
		c.timer.Stop()
	}
	c.timer = time.AfterFunc(c.h.limits.cursorIdle, func() {
		s.mu.Lock()
		if c.busy || c.closed || c.prefetching {
			s.mu.Unlock()
			return
		}
		rows := s.detachCursorLocked(c)
		s.mu.Unlock()
		if rows != nil {
			_ = rows.Close()
		}
	})
}

// --- fetch / closeCursor ---

type fetchParams struct {
	Handle   string `json:"handle"`
	Cursor   string `json:"cursor"`
	MaxRows  int    `json:"maxRows"`
	MaxBytes int    `json:"maxBytes"`
}

func (s *Server) startFetch(params json.RawMessage) (func() (any, error), *rpc.Error) {
	var p fetchParams
	if e := decodeParams(params, &p); e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h, e := s.handleLocked(p.Handle)
	if e != nil {
		return nil, e
	}
	b, e := h.budget(p.MaxRows, p.MaxBytes)
	if e != nil {
		return nil, e
	}
	c := h.cursors[p.Cursor]
	if c == nil || c.rows == nil {
		return nil, invalidf("unknown or expired cursor")
	}
	if c.busy {
		return nil, rpcError(Errorf(KindBusy, "cursor is busy"), nil)
	}
	if e := s.admitLocked(h); e != nil {
		return nil, e
	}
	c.busy = true
	if c.timer != nil {
		c.timer.Stop()
	}
	r := c.req
	r.busy, r.done = true, make(chan struct{})
	var prefetched chan struct{}
	if c.prefetching {
		prefetched = c.prefetchDone
	}
	return func() (any, error) {
		defer func() {
			s.mu.Lock()
			c.busy = false
			s.startPrefetchLocked(c, b)
			s.armLocked(c)
			s.endCallLocked(h, r)
			s.mu.Unlock()
		}()
		if prefetched != nil {
			// Part of the page is being read already; wait until the prefetch
			// hands the cursor back.
			<-prefetched
			if err := r.ctx.Err(); err != nil {
				s.closeCursor(c)
				return nil, rpcError(classify(r.ctx, err, KindDatabase), nil)
			}
		}
		page, perr := s.page(r.ctx, c, b, func(rows pagePayload, notice, cursorID string, done bool) []byte {
			out := make([]byte, 0, len(rows.rows)+96)
			out = append(out, '{')
			out = rows.appendRows(out)
			return encodeTail(out, notice, "", cursorID, done)
		})
		if perr != nil {
			s.closeCursor(c)
			return nil, rpcError(perr, s.scrubber(h.secrets))
		}
		return page, nil
	}, nil
}

type cursorParams struct {
	Handle string `json:"handle"`
	Cursor string `json:"cursor"`
}

func (s *Server) startCloseCursor(params json.RawMessage) (func() (any, error), *rpc.Error) {
	var p cursorParams
	if e := decodeParams(params, &p); e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.handles[p.Handle]
	var c *cursor
	if h != nil {
		c = h.cursors[p.Cursor]
	}
	if c == nil {
		return func() (any, error) { return true, nil }, nil
	}
	if c.busy {
		// A fetch is running: stop it; it closes the cursor when it returns.
		if c.req != nil {
			c.req.cancel()
		}
		c.closeWhenIdle = true
		return func() (any, error) { return true, nil }, nil
	}
	rows := s.detachCursorLocked(c)
	return func() (any, error) {
		if rows != nil {
			_ = rows.Close()
		}
		return true, nil
	}, nil
}

// --- cancel ---

type cancelParams struct {
	Handle    string `json:"handle"`
	RequestID string `json:"requestId"`
}

type cancelResult struct {
	State string `json:"state"`
}

func (s *Server) startCancel(params json.RawMessage) (func() (any, error), *rpc.Error) {
	var p cancelParams
	if e := decodeParams(params, &p); e != nil {
		return nil, e
	}
	notFound := func() (any, error) { return cancelResult{"notFound"}, nil }
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.handles[p.Handle]
	if h == nil {
		return notFound, nil
	}
	r := h.requests[p.RequestID]
	if r == nil {
		return notFound, nil
	}
	r.cancel()
	if !r.busy {
		var rows Rows
		if r.cursor != nil {
			rows = s.detachCursorLocked(r.cursor)
		}
		delete(h.requests, r.id)
		return func() (any, error) {
			if rows != nil {
				_ = rows.Close()
			}
			return cancelResult{"confirmed"}, nil
		}, nil
	}
	done := r.done
	wait := s.o.CancelWait
	return func() (any, error) {
		t := time.NewTimer(wait)
		defer t.Stop()
		select {
		case <-done:
			return cancelResult{"confirmed"}, nil
		case <-t.C:
			return cancelResult{"requested"}, nil
		}
	}, nil
}

// --- metadata ---

type metadataParams struct {
	Handle   string       `json:"handle"`
	Kind     MetadataKind `json:"kind"`
	Database string       `json:"database"`
	Table    string       `json:"table"`
	Cursor   string       `json:"cursor"`
	MaxBytes int          `json:"maxBytes"`
}

func (s *Server) startMetadata(params json.RawMessage) (func() (any, error), *rpc.Error) {
	var p metadataParams
	if e := decodeParams(params, &p); e != nil {
		return nil, e
	}
	if !p.Kind.valid() {
		return nil, invalidf("invalid params: unknown metadata kind")
	}
	if p.MaxBytes <= 0 {
		return nil, invalidf("invalid params: maxBytes must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h, e := s.handleLocked(p.Handle)
	if e != nil {
		return nil, e
	}
	budget := min(p.MaxBytes, h.limits.pageMaxBytes, lineBudget)
	if p.Cursor != "" {
		c := h.cursors[p.Cursor]
		if c == nil || c.rows != nil || c.busy {
			return nil, invalidf("unknown or expired cursor")
		}
		if c.timer != nil {
			c.timer.Stop()
		}
		c.busy = true
		return func() (any, error) {
			out, perr := s.metadataPage(c, budget)
			s.mu.Lock()
			c.busy = false
			s.armLocked(c)
			s.mu.Unlock()
			if perr != nil {
				s.closeCursor(c)
				return nil, rpcError(perr, nil)
			}
			return out, nil
		}, nil
	}
	if e := s.admitLocked(h); e != nil {
		return nil, e
	}
	req := MetadataRequest{Kind: p.Kind, Database: p.Database, Table: p.Table}
	return func() (any, error) {
		defer s.release(h)
		scrub := s.scrubber(h.secrets)
		mp, ok := h.conn.(MetadataProvider)
		if !ok {
			return nil, rpcError(ErrUnsupported, nil)
		}
		ctx, cancel := context.WithTimeout(h.ctx, s.o.MetadataTimeout)
		defer cancel()
		res, err := mp.Metadata(ctx, req)
		if err != nil {
			return nil, rpcError(classify(ctx, err, KindDatabase), scrub)
		}
		if res == nil {
			res = &MetadataResult{}
		}
		if !req.Kind.IsList() {
			v, err := jsonx.Marshal(res.Value)
			if err != nil {
				return nil, rpcError(Errorf(KindDatabase, "metadata value does not encode as JSON"), nil)
			}
			out := append(append([]byte(`{"value":`), v...), '}')
			if len(out) > min(p.MaxBytes, lineBudget) {
				return nil, rpcError(Errorf(KindDatabase, "metadata value exceeds the %d byte limit", min(p.MaxBytes, lineBudget)), nil)
			}
			return json.RawMessage(out), nil
		}
		items := make([][]byte, 0, len(res.Items))
		for _, it := range res.Items {
			v, err := jsonx.Marshal(it)
			if err != nil {
				return nil, rpcError(Errorf(KindDatabase, "metadata item does not encode as JSON"), nil)
			}
			items = append(items, v)
		}
		s.mu.Lock()
		s.nextCursor++
		c := &cursor{id: "m" + strconv.FormatUint(s.nextCursor, 10), h: h, items: items, busy: true}
		h.cursors[c.id] = c
		s.mu.Unlock()
		out, perr := s.metadataPage(c, budget)
		s.mu.Lock()
		c.busy = false
		s.armLocked(c)
		s.mu.Unlock()
		if perr != nil {
			s.closeCursor(c)
			return nil, rpcError(perr, nil)
		}
		return out, nil
	}, nil
}

// metadataPage answers the next items within budget; the cursor closes on
// the last page.
func (s *Server) metadataPage(c *cursor, budget int) (json.RawMessage, *Error) {
	fixed := len(`{"items":[],"cursor":`) + len(c.id) + 3
	out := make([]byte, 0, 1024)
	out = append(out, `{"items":[`...)
	n := 0
	for len(c.items) > 0 {
		it := c.items[0]
		add := len(it)
		if n > 0 {
			add++
		}
		if fixed+len(out)-len(`{"items":[`)+add > budget {
			if n == 0 {
				return nil, Errorf(KindDatabase, "metadata item exceeds the %d byte page limit", budget)
			}
			break
		}
		if n > 0 {
			out = append(out, ',')
		}
		out = append(out, it...)
		c.items = c.items[1:]
		n++
	}
	out = append(out, ']')
	if len(c.items) > 0 {
		out = append(out, `,"cursor":`...)
		out = appendJSONString(out, c.id)
		out = append(out, '}')
		return out, nil
	}
	out = append(out, '}')
	s.closeCursor(c)
	return out, nil
}

// --- mutate / destructive ---

type mutateParams struct {
	Handle     string         `json:"handle"`
	RequestID  string         `json:"requestId"`
	Op         MutationOp     `json:"op"`
	Database   string         `json:"database"`
	Table      string         `json:"table"`
	PrimaryKey map[string]any `json:"primaryKey"`
	Column     string         `json:"column"`
	Value      any            `json:"value"`
	Fields     map[string]any `json:"fields"`
}

func (s *Server) startMutate(params json.RawMessage) (func() (any, error), *rpc.Error) {
	var p mutateParams
	if e := decodeParams(params, &p); e != nil {
		return nil, e
	}
	switch p.Op {
	case MutationUpdateCell, MutationInsertRow, MutationDeleteRow:
	default:
		return nil, invalidf("invalid params: unknown mutation op")
	}
	if p.Table == "" {
		return nil, invalidf("invalid params: table is required")
	}
	m := Mutation{RequestID: p.RequestID, Op: p.Op, Database: p.Database, Table: p.Table, PrimaryKey: p.PrimaryKey, Column: p.Column, Value: p.Value, Fields: p.Fields}
	return s.startSimple(p.Handle, p.RequestID, func(ctx context.Context, conn Conn) (any, error) {
		mu, ok := conn.(Mutator)
		if !ok {
			return nil, ErrUnsupported
		}
		n, err := mu.Mutate(ctx, m)
		if err != nil {
			return nil, err
		}
		return struct {
			AffectedRows int64 `json:"affectedRows"`
		}{n}, nil
	})
}

type destructiveParams struct {
	Handle    string            `json:"handle"`
	RequestID string            `json:"requestId"`
	Action    DestructiveAction `json:"action"`
	Database  string            `json:"database"`
	Table     string            `json:"table"`
}

func (s *Server) startDestructive(params json.RawMessage) (func() (any, error), *rpc.Error) {
	var p destructiveParams
	if e := decodeParams(params, &p); e != nil {
		return nil, e
	}
	switch p.Action {
	case DestructiveDropTable, DestructiveClearTable:
		if p.Table == "" {
			return nil, invalidf("invalid params: table is required")
		}
	case DestructiveDropDatabase, DestructiveClearDatabase:
		if p.Table != "" || p.Database == "" {
			return nil, invalidf("invalid params: database actions take a database and no table")
		}
	default:
		return nil, invalidf("invalid params: unknown destructive action")
	}
	req := DestructiveRequest{RequestID: p.RequestID, Action: p.Action, Database: p.Database, Table: p.Table}
	return s.startSimple(p.Handle, p.RequestID, func(ctx context.Context, conn Conn) (any, error) {
		d, ok := conn.(Destroyer)
		if !ok {
			return nil, ErrUnsupported
		}
		if err := d.Destroy(ctx, req); err != nil {
			return nil, err
		}
		return json.RawMessage(`{"ok":true}`), nil
	})
}

// startSimple runs a cancellable request without a result set.
func (s *Server) startSimple(handleID, requestID string, call func(context.Context, Conn) (any, error)) (func() (any, error), *rpc.Error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, e := s.handleLocked(handleID)
	if e != nil {
		return nil, e
	}
	r, e := s.beginRequestLocked(h, requestID, s.o.WriteTimeout)
	if e != nil {
		return nil, e
	}
	return func() (any, error) {
		defer func() {
			s.mu.Lock()
			s.endCallLocked(h, r)
			s.mu.Unlock()
		}()
		out, err := call(r.ctx, h.conn)
		if err != nil {
			return nil, rpcError(classify(r.ctx, err, KindDatabase), s.scrubber(h.secrets))
		}
		return out, nil
	}, nil
}

// --- close / shutdown ---

type closeParams struct {
	Handle string `json:"handle"`
}

func (s *Server) startClose(params json.RawMessage) (func() (any, error), *rpc.Error) {
	var p closeParams
	if e := decodeParams(params, &p); e != nil {
		return nil, e
	}
	s.mu.Lock()
	h := s.handles[p.Handle]
	if h != nil {
		delete(s.handles, p.Handle)
		h.closed = true
		h.cancel(errHandleClosed)
	}
	s.mu.Unlock()
	return func() (any, error) {
		if h != nil {
			s.closeHandle(h, s.o.CloseWait)
		}
		return true, nil
	}, nil
}

// closeHandle waits (bounded) for the handle's running calls, closes its
// cursors and then the connection.
func (s *Server) closeHandle(h *handle, wait time.Duration) {
	s.mu.Lock()
	idle := h.idle
	running := h.running
	s.mu.Unlock()
	if running > 0 {
		t := time.NewTimer(wait)
		select {
		case <-idle:
		case <-t.C:
		}
		t.Stop()
	}
	s.mu.Lock()
	var rows []Rows
	for _, c := range h.cursors {
		if r := s.detachCursorLocked(c); r != nil {
			rows = append(rows, r)
		}
	}
	for id, r := range h.requests {
		r.cancel()
		delete(h.requests, id)
	}
	s.mu.Unlock()
	for _, r := range rows {
		_ = r.Close()
	}
	_ = h.conn.Close()
}

// CloseDatabase implements rpc.DatabaseHandler: it closes every handle and
// cursor (plugin obligation on plugin.shutdown) and rejects later calls.
func (s *Server) CloseDatabase() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	handles := make([]*handle, 0, len(s.handles))
	for id, h := range s.handles {
		handles = append(handles, h)
		h.closed = true
		h.cancel(errHandleClosed)
		delete(s.handles, id)
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, h := range handles {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.closeHandle(h, s.o.CloseWait)
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	t := time.NewTimer(2 * s.o.CloseWait)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
	}
}

// OpenHandles reports the number of open connections (for tests).
func (s *Server) OpenHandles() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.handles)
}
