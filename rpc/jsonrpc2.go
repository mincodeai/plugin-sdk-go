package rpc

import (
	"bytes"
	"encoding/json"
)

// JSON-RPC 2.0 completeness behind Options.JSONRPC2: batches (§6) and the
// -32600 reply for parsable messages that are not valid requests (§5.1).
// Presets that accept lenient envelopes (no RequireVersion, e.g. NodeDefaults)
// keep that leniency: only non-object values are rejected with -32600 there.

// strictRequests: full request validation applies (JSONRPC2 + RequireVersion).
func (s *server) strictRequests() bool { return s.o.JSONRPC2 && s.o.RequireVersion }

func isJSONObject(line []byte) bool {
	trimmed := bytes.TrimLeft(line, " \t\r\n")
	return len(trimmed) > 0 && trimmed[0] == '{'
}

// invalidBatchEntry: an entry answered with -32600 inside the batch array.
func (s *server) invalidBatchEntry(env map[string]json.RawMessage, decoded bool) bool {
	return !decoded || (s.strictRequests() && !validJSONRPCRequest(env))
}

type replyBatch struct {
	items   [][]byte
	pending int  // registered ids still waiting for their reply
	sealed  bool // every entry has been dispatched
}

func (b *replyBatch) add(line []byte) {
	b.items = append(b.items, append([]byte(nil), line...))
	b.pending--
}

// takeBatchLocked removes and returns the oldest batch waiting for a reply
// with this id, or nil when the reply is not batched. Caller holds s.mu.
func (s *server) takeBatchLocked(id json.RawMessage) *replyBatch {
	queue := s.batches[string(id)]
	if len(queue) == 0 {
		return nil
	}
	if len(queue) == 1 {
		delete(s.batches, string(id))
	} else {
		s.batches[string(id)] = queue[1:]
	}
	return queue[0]
}

// registerBatchLocked records that the request being dispatched from the
// current batch will be answered, so its reply is collected into the array.
// handleLine calls it once it has decided to answer the request, before any
// reply can be written. Caller holds s.mu.
func (s *server) registerBatchLocked(id json.RawMessage) {
	if s.dispatch == nil {
		return
	}
	if s.batches == nil {
		s.batches = map[string][]*replyBatch{}
	}
	s.batches[string(id)] = append(s.batches[string(id)], s.dispatch)
	s.dispatch.pending++
}

// shutdownReplyLocked answers plugin.shutdown as the last reply: batches still
// waiting for requests that will never be answered are written first, then
// the shutdown reply, which joins its own batch when it was batched (that
// array is written by handleBatch once sealed). Caller holds s.mu.
func (s *server) shutdownReplyLocked(id json.RawMessage) {
	own := s.takeBatchLocked(id)
	s.abandonBatchesLocked()
	if own != nil {
		s.batches = map[string][]*replyBatch{string(id): {own}}
		own.pending = 1
	}
	s.replyLocked(id, json.RawMessage("{}"), nil)
}

// abandonBatchesLocked runs once replies stop (plugin.shutdown): requests
// still running will not be answered, so no open batch may wait for them.
// Sealed batches are written with the replies they collected; the batch being
// dispatched is written by handleBatch once sealed. Caller holds s.mu.
func (s *server) abandonBatchesLocked() {
	open := map[*replyBatch]struct{}{}
	for _, queue := range s.batches {
		for _, b := range queue {
			open[b] = struct{}{}
		}
	}
	s.batches = nil
	for b := range open {
		b.pending = 0
		s.flushBatchLocked(b, false)
	}
}

// protocolReplyLocked answers a message that is not a valid request (-32700 /
// -32600). It is never matched to a pending batched request with the same id:
// while a batch is dispatched it joins that batch's array, otherwise it is
// written on its own. Caller holds s.mu.
func (s *server) protocolReplyLocked(line []byte) {
	if s.dispatch != nil {
		s.dispatch.items = append(s.dispatch.items, append([]byte(nil), line...))
		return
	}
	_ = s.writeLocked(line)
}

// validJSONRPCRequest accepts a response (result or error without method) or a
// request with version "2.0", a non-empty string method, a string / number /
// null id when present, and object or array params when present.
//
// A message without a method is handled as a (possibly malformed) response and
// never answered: on this two-way pipe an error echoing its id could be taken
// by the peer as the reply to one of its own pending calls.
func validJSONRPCRequest(env map[string]json.RawMessage) bool {
	method, hasMethod := env["method"]
	if !hasMethod || string(method) == `""` || string(method) == "null" {
		return true
	}
	var version, name string
	if json.Unmarshal(env["jsonrpc"], &version) != nil || version != "2.0" || json.Unmarshal(method, &name) != nil || name == "" {
		return false
	}
	if id, ok := env["id"]; ok && !validID(id) {
		return false
	}
	if params, ok := env["params"]; ok {
		trimmed := bytes.TrimSpace(params)
		if len(trimmed) == 0 || (trimmed[0] != '{' && trimmed[0] != '[') {
			return false
		}
	}
	return true
}

func validID(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return false
	}
	switch raw[0] {
	case '"':
		var s string
		return json.Unmarshal(raw, &s) == nil
	case 'n':
		return string(raw) == "null"
	case '{', '[', 't', 'f':
		return false
	}
	var n json.Number
	return json.Unmarshal(raw, &n) == nil
}

// invalidRequest answers -32600, echoing id when it is a valid id.
func (s *server) invalidRequest(id json.RawMessage) {
	if !validID(id) {
		id = json.RawMessage("null")
	}
	line, _ := s.encodeReply(id, nil, NewError(CodeInvalidRequest, "Invalid Request"))
	s.mu.Lock()
	s.protocolReplyLocked(line)
	s.mu.Unlock()
}

// handleBatch dispatches each entry like a single line, collecting the replies
// of its requests into one array written once all have answered.
func (s *server) handleBatch(line []byte) bool {
	var items []json.RawMessage
	if json.Unmarshal(line, &items) != nil {
		s.invalid()
		return false
	}
	if len(items) == 0 {
		s.invalidRequest(nil)
		return false
	}
	batch := &replyBatch{}
	end := false
	for _, item := range items {
		var env map[string]json.RawMessage
		decoded := json.Unmarshal(item, &env) == nil
		if s.invalidBatchEntry(env, decoded) {
			reply, _ := s.encodeReply(invalidID(env), nil, NewError(CodeInvalidRequest, "Invalid Request"))
			s.mu.Lock()
			batch.items = append(batch.items, reply)
			s.mu.Unlock()
			continue
		}
		if end {
			continue // nothing after plugin.shutdown is served
		}
		// handleLine registers each request it will answer (see
		// registerBatchLocked) and routes protocol errors into the batch.
		s.mu.Lock()
		s.dispatch = batch
		s.mu.Unlock()
		end = s.handleLine(item)
		s.mu.Lock()
		s.dispatch = nil
		s.mu.Unlock()
	}
	s.mu.Lock()
	batch.sealed = true
	// After a batched plugin.shutdown the transport is finished, but the
	// array holding its reply must still go out.
	s.flushBatchLocked(batch, end)
	s.mu.Unlock()
	return end
}

func invalidID(env map[string]json.RawMessage) json.RawMessage {
	if id, ok := env["id"]; ok && validID(id) {
		return id
	}
	return json.RawMessage("null")
}

// flushBatchLocked writes the array once the batch is sealed and every
// registered request answered; a batch with nothing to answer writes nothing.
// final allows the write after the transport finished (batched shutdown).
func (s *server) flushBatchLocked(batch *replyBatch, final bool) {
	if !batch.sealed {
		return
	}
	if batch.pending > 0 {
		return
	}
	if len(batch.items) == 0 {
		return
	}
	line := append([]byte{'['}, bytes.Join(batch.items, []byte{','})...)
	line = append(line, ']')
	batch.items = nil
	if final {
		_ = s.writeOutLocked(s.frameLocked(line))
		return
	}
	_ = s.writeLocked(line)
}
