package rpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/jsonx"
)

var (
	errLineTooLong = errors.New("rpc: input line too long")
	errWrite       = errors.New("rpc: transport write failed")
	errRead        = errors.New("rpc: transport read failed")
)

type server struct {
	o      Options
	out    io.Writer
	errOut io.Writer

	ctx    context.Context
	cancel context.CancelFunc
	host   *host.Client
	tasks  *taskTable
	wg     sync.WaitGroup

	mu           sync.Mutex
	writeErr     error
	replyStopped bool // in-flight replies and host requests are dropped
	finished     bool // nothing more is written
	initialized  bool
	negotiated   bool
	inflight     int
	closeOnce    sync.Once
}

func (o *Options) setDefaults() {
	if o.MaxLineBytes <= 0 {
		o.MaxLineBytes = 1 << 20
	}
	if o.InvalidMessage == "" {
		if o.Invalid == InvalidReply {
			o.InvalidMessage = "Invalid JSON-RPC request"
		} else {
			o.InvalidMessage = "Invalid plugin RPC request"
		}
	}
	if o.MaxIDBytes <= 0 {
		o.MaxIDBytes = 256
	}
	if o.MethodNotFound == "" {
		o.MethodNotFound = "Method not found"
	}
	if o.NotInitialized == nil {
		o.NotInitialized = NewError(CodeServerError, "Plugin not initialized")
	}
	if o.MaxInflight <= 0 {
		o.MaxInflight = 1
	}
	if o.Busy == nil {
		o.Busy = NewError(CodeBusy, "Plugin busy, try again later")
	}
	if o.InvalidParams == nil {
		o.InvalidParams = NewError(CodeInvalidParams, "Invalid invoke parameters")
	}
	if o.UnknownAction == nil {
		o.UnknownAction = func(string) *Error { return NewError(CodeMethodNotFound, "Unknown action") }
	}
	if o.PayloadNotObject == nil {
		o.PayloadNotObject = NewError(CodeInvalidParams, "Payload must be a JSON object")
	}
	if o.PayloadInvalidJSON == nil {
		o.PayloadInvalidJSON = NewError(CodeInvalidParams, "Payload must contain valid JSON")
	}
	if o.ResultTooLarge == nil {
		o.ResultTooLarge = NewError(CodeServerError, "Result too large")
	}
	if o.PanicError == nil {
		o.PanicError = NewError(CodeServerError, "Internal error")
	}
	if o.TasksNotNegotiated == "" {
		o.TasksNotNegotiated = "Tasks were not negotiated"
	}
	if o.DrainTimeout <= 0 {
		o.DrainTimeout = 1500 * time.Millisecond
	}
}

// Serve runs the JSON-RPC loop until EOF, plugin.shutdown, a transport error
// or ctx cancellation (which only stops in-flight work; close stdin to stop
// reading). It returns nil after a clean shutdown or EOF.
func Serve(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, o Options) error {
	o.setDefaults()
	if o.Handler == nil {
		return errors.New("rpc: Options.Handler is required")
	}
	s := &server{o: o, out: stdout, errOut: stderr}
	s.ctx, s.cancel = context.WithCancel(ctx)
	defer s.cancel()
	ho := o.Host
	ho.Done = s.ctx.Done()
	if ho.IDPrefix == "" {
		ho.IDPrefix = "host-"
	}
	s.host = host.NewClient(s.sendHost, ho)
	if o.Tasks != nil {
		s.tasks = newTaskTable(o.Tasks)
	}
	err := s.loop(stdin)
	if err != nil && o.FatalMessage != "" && stderr != nil {
		_, _ = io.WriteString(stderr, o.FatalMessage+"\n")
	}
	return err
}

func (s *server) loop(stdin io.Reader) error {
	r := bufio.NewReaderSize(stdin, 64*1024)
	for {
		line, overlong, err := s.readLine(r)
		if overlong {
			if s.o.OverlongFatal {
				s.stopEOF()
				return errLineTooLong
			}
			s.invalid()
		} else if line != nil {
			if s.handleLine(line) {
				return s.result()
			}
		}
		if err != nil {
			s.stopEOF()
			if err != io.EOF {
				return errRead
			}
			return s.result()
		}
	}
}

func (s *server) result() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeErr
}

// readLine returns the next line without its LF (and one trailing CR). line
// is nil at EOF without data. overlong reports a line beyond the limit (its
// bytes are discarded).
func (s *server) readLine(r *bufio.Reader) (line []byte, overlong bool, err error) {
	max := s.o.MaxLineBytes
	var buf []byte
	total := 0
	for {
		chunk, e := r.ReadSlice('\n')
		if f := s.o.FatalLineBytes; f > 0 && !s.o.OverlongFatal {
			// bufio.Scanner with a larger buffer: the line and its terminator
			// must fit in FatalLineBytes, else the transport fails.
			total += len(chunk)
			if total > f || (total == f && (len(chunk) == 0 || chunk[len(chunk)-1] != '\n')) {
				return nil, false, errLineTooLong
			}
		}
		if !overlong {
			if s.o.OverlongFatal {
				// bufio.Scanner: the line and its terminator must fit in max bytes.
				n := len(buf) + len(chunk)
				if n > max || (n == max && (len(chunk) == 0 || chunk[len(chunk)-1] != '\n')) {
					return nil, true, nil
				} else {
					buf = append(buf, chunk...)
				}
			} else if len(buf)+len(chunk) > max+2 {
				overlong, buf = true, nil
			} else {
				buf = append(buf, chunk...)
			}
		}
		if errors.Is(e, bufio.ErrBufferFull) {
			continue
		}
		if overlong {
			return nil, true, e
		}
		if e != nil && len(buf) == 0 {
			return nil, false, e
		}
		buf = bytes.TrimSuffix(buf, []byte("\n"))
		buf = bytes.TrimSuffix(buf, []byte("\r"))
		if !s.o.OverlongFatal && len(buf) > max {
			return nil, true, e
		}
		if buf == nil {
			buf = []byte{}
		}
		return buf, false, e
	}
}

// write sends one line unless the transport finished. Caller holds s.mu.
func (s *server) writeLocked(line []byte) error {
	if s.finished || s.writeErr != nil {
		return errWrite
	}
	b := make([]byte, 0, len(line)+1)
	b = append(append(b, line...), '\n')
	if _, err := s.out.Write(b); err != nil {
		s.writeErr = errWrite
		s.cancel()
		return s.writeErr
	}
	return nil
}

func (s *server) replyLocked(id json.RawMessage, result json.RawMessage, e *Error) {
	line, err := s.encodeReply(id, result, e)
	if err == nil && s.o.MaxReplyBytes > 0 && len(line)+1 > s.o.MaxReplyBytes {
		line, err = s.encodeReply(id, nil, s.o.ResultTooLarge)
	}
	if err != nil {
		line, _ = s.encodeReply(id, nil, s.o.PanicError)
	}
	_ = s.writeLocked(line)
}

func (s *server) sendHost(line []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.replyStopped {
		return host.ErrUnavailable
	}
	return s.writeLocked(line)
}

func (s *server) invalid() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.o.Invalid == InvalidReply {
		if s.o.InvalidOmitID {
			line, _ := encodeJSON(struct {
				JSONRPC string     `json:"jsonrpc"`
				Error   *wireError `json:"error"`
			}{"2.0", &wireError{Code: CodeParseError, Message: s.o.InvalidMessage}}, s.o.EscapeHTML)
			_ = s.writeLocked(line)
			return
		}
		s.replyLocked(json.RawMessage("null"), nil, NewError(CodeParseError, s.o.InvalidMessage))
		return
	}
	if s.errOut != nil {
		_, _ = io.WriteString(s.errOut, s.o.InvalidMessage+"\n")
	}
}

// isFalsy reports whether a raw non-string JSON value is falsy in JS.
func isFalsy(raw json.RawMessage) bool {
	switch string(raw) {
	case "false", "null":
		return true
	}
	var f float64
	return json.Unmarshal(raw, &f) == nil && f == 0
}

func isBlank(b []byte) bool {
	return len(bytes.TrimLeft(b, " \t\r\n")) == 0
}

// handleLine processes one line and reports whether the loop must end.
func (s *server) handleLine(line []byte) bool {
	if len(line) == 0 && (s.o.SkipEmptyLines || s.o.SkipBlankLines) {
		return false
	}
	if s.o.SkipBlankLines && isBlank(line) {
		return false
	}
	if s.o.StrictJSON && !jsonx.Strict(line) {
		s.invalid()
		return false
	}
	env, ok := s.decodeEnvelope(line)
	if !ok {
		s.invalid()
		return false
	}
	if s.o.RequireVersion {
		var v string
		if json.Unmarshal(env["jsonrpc"], &v) != nil || v != "2.0" {
			s.invalid()
			return false
		}
	}
	method, response, nonString := "", false, false
	if raw, ok := env["method"]; ok && string(raw) != "null" {
		if json.Unmarshal(raw, &method) != nil {
			if s.o.NonStringMethod == MethodTypeTruthy && isFalsy(raw) {
				response = true
			} else {
				switch s.o.NonStringMethod {
				case MethodTypeInvalid:
					s.invalid()
					return false
				case MethodTypeIgnore:
					response = true
				default:
					nonString = true
				}
			}
		}
	}
	id, hasID := env["id"]
	if response || (method == "" && !nonString) {
		if hasID {
			var we *host.WireError
			if s.o.HostReplyError != nil {
				we = s.o.HostReplyError(env["error"])
			} else if raw, ok := env["error"]; ok && string(raw) != "null" {
				we = &host.WireError{}
				if json.Unmarshal(raw, we) != nil {
					we = &host.WireError{Code: CodeServerError}
				}
			}
			s.host.Deliver(id, env["result"], we)
		}
		return false
	}
	if !hasID || !s.acceptID(id) {
		return false
	}
	params := env["params"]
	if nonString {
		s.mu.Lock()
		s.replyLocked(id, nil, NewError(CodeMethodNotFound, s.o.MethodNotFound))
		s.mu.Unlock()
		return false
	}
	switch method {
	case "plugin.initialize":
		var p struct {
			Features struct {
				Tasks json.RawMessage `json:"tasks"`
			} `json:"features"`
		}
		_ = json.Unmarshal(params, &p)
		s.mu.Lock()
		s.initialized = true
		s.negotiated = s.tasks != nil && string(p.Features.Tasks) == "1"
		s.replyLocked(id, s.initializeResult(s.negotiated), nil)
		s.mu.Unlock()
	case "plugin.ping":
		s.mu.Lock()
		s.replyLocked(id, s.initializeResult(false), nil)
		s.mu.Unlock()
	case "plugin.shutdown":
		s.shutdown(id)
		return true
	case "plugin.invoke":
		s.invoke(id, params)
	case "plugin.task.start", "plugin.task.status", "plugin.task.cancel":
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.tasks == nil {
			s.replyLocked(id, nil, s.noTasksError())
			return false
		}
		if !s.initialized || !s.negotiated {
			s.replyLocked(id, nil, NewError(CodeMethodNotFound, s.o.TasksNotNegotiated))
			return false
		}
		if method == "plugin.task.start" {
			s.taskStartLocked(id, params)
		} else {
			s.taskLookupLocked(id, params, method == "plugin.task.cancel")
		}
	default:
		e := NewError(CodeMethodNotFound, s.o.MethodNotFound)
		if strings.HasPrefix(method, "plugin.task.") && s.tasks == nil {
			e = s.noTasksError()
		}
		s.mu.Lock()
		s.replyLocked(id, nil, e)
		s.mu.Unlock()
	}
	return false
}

// decodeEnvelope splits a line into its members. With StructEnvelope the
// line is decoded like the Go plugins' original request struct: member names
// match case-insensitively (the last match wins), "jsonrpc" and "method" must
// be strings, "error" must be null or {code int, message string}, and an empty
// method string counts as absent.
func (s *server) decodeEnvelope(line []byte) (map[string]json.RawMessage, bool) {
	if s.o.DecodeEnvelope != nil {
		env, ok := s.o.DecodeEnvelope(line)
		return env, ok && env != nil
	}
	if !s.o.StructEnvelope {
		var env map[string]json.RawMessage
		if json.Unmarshal(line, &env) != nil || env == nil {
			return nil, false
		}
		return env, true
	}
	if s.o.LenientErrorMember {
		return decodeLenientEnvelope(line)
	}
	var r struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
		Result  json.RawMessage `json:"result"`
		Error   *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(line, &r) != nil {
		return nil, false
	}
	env := map[string]json.RawMessage{}
	if r.JSONRPC != "" {
		env["jsonrpc"], _ = json.Marshal(r.JSONRPC)
	}
	if r.Method != "" {
		env["method"], _ = json.Marshal(r.Method)
	}
	for k, v := range map[string]json.RawMessage{"id": r.ID, "params": r.Params, "result": r.Result} {
		if v != nil {
			env[k] = v
		}
	}
	if r.Error != nil {
		env["error"], _ = json.Marshal(r.Error)
	}
	return env, true
}

// decodeLenientEnvelope is decodeEnvelope for StructEnvelope plus
// LenientErrorMember: the request struct has no result/error members, and a
// host reply (no method) is decoded apart as {result, error}; a reply whose
// error member does not decode is dropped.
func decodeLenientEnvelope(line []byte) (map[string]json.RawMessage, bool) {
	var r struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
	}
	if json.Unmarshal(line, &r) != nil {
		return nil, false
	}
	env := map[string]json.RawMessage{}
	if r.JSONRPC != "" {
		env["jsonrpc"], _ = json.Marshal(r.JSONRPC)
	}
	if r.Method != "" {
		env["method"], _ = json.Marshal(r.Method)
	}
	for k, v := range map[string]json.RawMessage{"id": r.ID, "params": r.Params} {
		if v != nil {
			env[k] = v
		}
	}
	if r.Method != "" {
		return env, true
	}
	var reply struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(line, &reply) != nil {
		delete(env, "id") // undeliverable: ignored like an id-less reply
		return env, true
	}
	if reply.Result != nil {
		env["result"] = reply.Result
	}
	if reply.Error != nil {
		env["error"], _ = json.Marshal(reply.Error)
	}
	return env, true
}

// noTasksError answers plugin.task.* when the plugin has no task support.
func (s *server) noTasksError() *Error {
	if s.o.RejectTaskMethods {
		return NewError(CodeMethodNotFound, s.o.TasksNotNegotiated)
	}
	return NewError(CodeMethodNotFound, s.o.MethodNotFound)
}

func (s *server) acceptID(id json.RawMessage) bool {
	switch s.o.IDs {
	case IDAny:
		return true
	case IDNonNull:
		return string(id) != "null"
	}
	if len(id) == 0 || len(id) > s.o.MaxIDBytes {
		return false
	}
	c := id[0]
	return c == '"' || c == '-' || c >= '0' && c <= '9'
}

// invokeParams decodes invoke (or task) params into action and payload.
type invokeParams struct {
	action  string
	payload json.RawMessage
}

func (s *server) parseParams(params json.RawMessage) (invokeParams, *Error) {
	if s.o.Payload == PayloadObject && s.o.StructParams {
		var q struct {
			Action  string          `json:"action"`
			Payload json.RawMessage `json:"payload"`
		}
		_ = json.Unmarshal(params, &q)
		return invokeParams{action: q.Action, payload: q.Payload}, nil
	}
	if s.o.Payload == PayloadObject {
		var m map[string]json.RawMessage
		_ = json.Unmarshal(params, &m)
		var p invokeParams
		_ = json.Unmarshal(m["action"], &p.action)
		p.payload = m["payload"]
		return p, nil
	}
	var p struct {
		Action  string `json:"action"`
		Payload string `json:"payload"`
	}
	if json.Unmarshal(params, &p) != nil {
		return invokeParams{}, s.o.InvalidParams
	}
	if p.Payload == "" && !s.o.KeepEmptyPayload {
		p.Payload = "{}"
	}
	return invokeParams{action: p.Action, payload: json.RawMessage(p.Payload)}, nil
}

// normalizePayload applies PayloadObject rules (no-op for PayloadString).
func (s *server) normalizePayload(raw json.RawMessage) (json.RawMessage, *Error) {
	if s.o.Payload != PayloadObject {
		return raw, nil
	}
	if s.o.StrictPayload {
		text := []byte("{}")
		if len(raw) > 0 && string(raw) != "null" {
			var str string
			if json.Unmarshal(raw, &str) == nil {
				if str != "" {
					text = []byte(str)
				}
			} else {
				text = raw
			}
		}
		if !jsonx.Strict(text) {
			return nil, s.o.PayloadInvalidJSON
		}
		if !jsonx.IsObject(text) {
			return nil, s.o.PayloadNotObject
		}
		return json.RawMessage(text), nil
	}
	if len(raw) == 0 || string(raw) == "null" {
		return json.RawMessage("{}"), nil
	}
	switch raw[0] {
	case '{':
		return raw, nil
	case '"':
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return nil, s.o.PayloadInvalidJSON
		}
		if text == "" {
			return json.RawMessage("{}"), nil
		}
		if !json.Valid([]byte(text)) {
			return nil, s.o.PayloadInvalidJSON
		}
		if !jsonx.IsObject([]byte(text)) {
			return nil, s.o.PayloadNotObject
		}
		return json.RawMessage(text), nil
	}
	return nil, s.o.PayloadNotObject
}

// validateInvoke runs the shared admission checks (busy, params, action,
// payload). Caller holds s.mu.
func (s *server) validateInvokeLocked(params json.RawMessage) (invokeParams, *Error) {
	var p invokeParams
	var perr *Error
	if s.o.ParamsBeforeBusy {
		if p, perr = s.parseParams(params); perr != nil {
			return p, perr
		}
	}
	if s.inflight >= s.o.MaxInflight {
		return p, s.o.Busy
	}
	if !s.o.ParamsBeforeBusy {
		if p, perr = s.parseParams(params); perr != nil {
			return p, perr
		}
	}
	if s.o.KnownAction != nil && !s.o.KnownAction(p.action) {
		return p, s.o.UnknownAction(p.action)
	}
	payload, perr := s.normalizePayload(p.payload)
	if perr != nil {
		return p, perr
	}
	p.payload = payload
	if s.o.Admit != nil {
		if e := s.o.Admit(p.action, p.payload); e != nil {
			return p, e
		}
	}
	return p, nil
}

func (s *server) invoke(id json.RawMessage, params json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.o.RequireInitialize && !s.initialized {
		s.replyLocked(id, nil, s.o.NotInitialized)
		return
	}
	p, perr := s.validateInvokeLocked(params)
	if perr != nil {
		s.replyLocked(id, nil, perr)
		return
	}
	s.inflight++
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		result, err := s.call(s.ctx, p.action, p.payload)
		var rerr *Error
		var raw json.RawMessage
		if err != nil {
			rerr = s.mapError(err)
		} else if raw, err = s.marshalResult(result); err != nil || !json.Valid(raw) || (s.o.MaxResultBytes > 0 && len(raw) > s.o.MaxResultBytes) {
			rerr = s.o.ResultTooLarge
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.inflight--
		if !s.replyStopped {
			s.replyLocked(id, raw, rerr)
		}
	}()
}

// call runs the handler with the host caller attached, converting panics.
func (s *server) call(ctx context.Context, action string, payload json.RawMessage) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			result, err = nil, &panicError{r}
		}
	}()
	return s.o.Handler(host.WithCaller(ctx, s.host), action, payload)
}

type panicError struct{ v any }

func (p *panicError) Error() string { return fmt.Sprint("panic: ", p.v) }

func (s *server) mapError(err error) *Error {
	var pe *panicError
	if errors.As(err, &pe) {
		return s.o.PanicError
	}
	var re *Error
	if errors.As(err, &re) {
		return re
	}
	if s.o.MapError != nil {
		if m := s.o.MapError(err); m != nil {
			return m
		}
	}
	return NewError(CodeServerError, err.Error())
}

// waitDrain waits for in-flight work up to DrainTimeout (without limit when
// Options.WaitAll).
func (s *server) waitDrain() {
	if s.o.WaitAll {
		s.wg.Wait()
		return
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	t := time.NewTimer(s.o.DrainTimeout)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
	}
}

func (s *server) closeAll() {
	s.closeOnce.Do(func() {
		s.host.Close(false)
		if s.o.OnClose != nil {
			s.o.OnClose()
		}
	})
}

func (s *server) shutdown(id json.RawMessage) {
	if s.o.DrainReplies {
		s.cancel()
		s.waitDrain()
		s.closeAll()
		s.mu.Lock()
		s.replyLocked(id, json.RawMessage("{}"), nil)
		s.finished, s.replyStopped = true, true
		s.mu.Unlock()
		return
	}
	s.mu.Lock()
	s.replyStopped = true
	s.cancel()
	s.host.Close(false)
	if s.tasks != nil {
		s.tasks.cancelAllLocked()
	}
	if !s.o.ShutdownReplyLast {
		s.replyLocked(id, json.RawMessage("{}"), nil)
		s.finished = true
	}
	s.mu.Unlock()
	s.waitDrain()
	if s.o.ShutdownReplyLast {
		if s.o.CloseBeforeShutdownReply {
			s.closeAll()
		}
		s.mu.Lock()
		s.replyLocked(id, json.RawMessage("{}"), nil)
		s.finished = true
		s.mu.Unlock()
	}
	s.closeAll()
}

func (s *server) stopEOF() {
	if s.o.DrainReplies {
		s.cancel()
		s.waitDrain()
		s.closeAll()
		s.mu.Lock()
		s.finished, s.replyStopped = true, true
		s.mu.Unlock()
		return
	}
	s.mu.Lock()
	s.replyStopped = true
	s.cancel()
	if s.tasks != nil {
		s.tasks.cancelAllLocked()
	}
	s.mu.Unlock()
	s.waitDrain()
	s.closeAll()
	s.mu.Lock()
	s.finished = true
	s.mu.Unlock()
}

type progressKey struct{}

// ReportProgress publishes task progress (a JSON-marshalable value) from a
// handler running as a task; it is a no-op for direct invokes.
func ReportProgress(ctx context.Context, v any) {
	if f, ok := ctx.Value(progressKey{}).(func(any)); ok {
		f(v)
	}
}

// InTask reports whether ctx belongs to a handler running as a task
// (plugin.task.start) rather than a direct invoke.
func InTask(ctx context.Context) bool {
	_, ok := ctx.Value(progressKey{}).(func(any))
	return ok
}
