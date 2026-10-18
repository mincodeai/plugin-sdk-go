package rpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"

	"github.com/mincodeai/plugin-sdk-go/host"
)

// MethodFunc handles one plugin.call from a plugin view.
type MethodFunc func(ctx context.Context, params json.RawMessage) (any, error)

// Identity is the plugin id (and optionally version) reported to the host.
type Identity struct {
	ID      string `json:"id"`
	Version string `json:"version,omitempty"`
}

// Set with -ldflags -X by build scripts (see Options.Identity).
var (
	BuildPluginID      string
	BuildPluginVersion string
)

// TransportFramed selects length-prefixed frames (see Options.Transport).
const TransportFramed = "framed"

const (
	frameJSON        byte = 0
	frameBinary      byte = 1
	frameHeaderBytes      = 5
	// MaxBinaryBytes bounds one binary frame; chunk larger transfers.
	MaxBinaryBytes = 8 * 1024 * 1024
	maxChannelLen  = 128
)

var channelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)

var (
	// ErrBinaryUnavailable is returned by SendBinary outside a framed server.
	ErrBinaryUnavailable = errors.New("rpc: binary frames need Options.Transport = \"framed\" and a matching manifest runtime.transport")
	errFrame             = errors.New("rpc: invalid frame")
)

type binaryKey struct{}

func (s *server) framed() bool { return s.o.Transport == TransportFramed }

func (s *server) identity() *Identity {
	if s.o.Identity != nil && s.o.Identity.ID != "" {
		return s.o.Identity
	}
	if BuildPluginID != "" {
		return &Identity{ID: BuildPluginID, Version: BuildPluginVersion}
	}
	return nil
}

// withIdentity appends "plugin" to an initialize result object, keeping the
// existing members byte-for-byte.
func withIdentity(result json.RawMessage, identity *Identity) json.RawMessage {
	encoded, err := json.Marshal(identity)
	if err != nil || len(result) < 2 || result[len(result)-1] != '}' {
		return result
	}
	out := make([]byte, 0, len(result)+len(encoded)+12)
	out = append(out, result[:len(result)-1]...)
	if len(bytes.TrimSpace(result[1:len(result)-1])) > 0 {
		out = append(out, ',')
	}
	out = append(out, `"plugin":`...)
	out = append(out, encoded...)
	return append(out, '}')
}

// SendBinary sends one binary frame to the plugin's open views. ctx must come
// from the server (handler, task, OnInitialize, OnBinary or OnEvent context).
func SendBinary(ctx context.Context, channel string, data []byte) error {
	s, _ := ctx.Value(binaryKey{}).(*server)
	if s == nil || !s.framed() {
		return ErrBinaryUnavailable
	}
	frame, err := encodeBinaryFrame(channel, data)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.replyStopped {
		return host.ErrUnavailable
	}
	return s.writeRawLocked(frame)
}

func encodeJSONFrame(message []byte) []byte {
	frame := make([]byte, frameHeaderBytes+len(message))
	frame[0] = frameJSON
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(message)))
	copy(frame[frameHeaderBytes:], message)
	return frame
}

func encodeBinaryFrame(channel string, data []byte) ([]byte, error) {
	if len(channel) > maxChannelLen || !channelPattern.MatchString(channel) {
		return nil, fmt.Errorf("rpc: invalid binary channel %q", channel)
	}
	if len(data) > MaxBinaryBytes {
		return nil, fmt.Errorf("rpc: binary payload exceeds %d bytes; chunk the transfer", MaxBinaryBytes)
	}
	length := 2 + len(channel) + len(data)
	frame := make([]byte, frameHeaderBytes+length)
	frame[0] = frameBinary
	binary.BigEndian.PutUint32(frame[1:5], uint32(length))
	binary.BigEndian.PutUint16(frame[5:7], uint16(len(channel)))
	copy(frame[7:], channel)
	copy(frame[7+len(channel):], data)
	return frame, nil
}

// frameLoop is loop for the framed transport. JSON frames go through
// handleLine; binary frames are handed to OnBinary in order.
func (s *server) frameLoop(r *bufio.Reader) error {
	// A full queue blocks reading (backpressure). Serve does not wait for a
	// handler still running at shutdown.
	binaries := make(chan [2]any, 64)
	go func() {
		for frame := range binaries {
			if s.o.OnBinary != nil {
				s.deliverBinary(frame[0].(string), frame[1].([]byte))
			}
		}
	}()
	defer close(binaries)
	var header [frameHeaderBytes]byte
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			s.stopEOF()
			if errors.Is(err, io.EOF) {
				return s.result()
			}
			return errRead
		}
		kind, length := header[0], int(binary.BigEndian.Uint32(header[1:5]))
		switch {
		case kind == frameJSON && length <= s.o.MaxLineBytes:
		case kind == frameBinary && length >= 2 && length <= 2+maxChannelLen+MaxBinaryBytes:
		default:
			// An unknown kind or oversized frame desynchronizes the stream.
			s.stopEOF()
			return errFrame
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			s.stopEOF()
			return errRead
		}
		if kind == frameJSON {
			if s.handleLine(payload) {
				return s.result()
			}
			continue
		}
		channelLength := int(binary.BigEndian.Uint16(payload[:2]))
		if 2+channelLength > len(payload) || !channelPattern.MatchString(string(payload[2:2+channelLength])) {
			s.stopEOF()
			return errFrame
		}
		binaries <- [2]any{string(payload[2 : 2+channelLength]), payload[2+channelLength:]}
	}
}

func (s *server) deliverBinary(channel string, data []byte) {
	defer func() {
		if r := recover(); r != nil && s.errOut != nil {
			_, _ = fmt.Fprintf(s.errOut, "plugin binary handler panicked: %v\n", r)
		}
	}()
	s.o.OnBinary(host.WithCaller(s.ctx, s.host), channel, data)
}

// pluginCall runs a plugin.call {method, params} request from a view.
func (s *server) pluginCall(id json.RawMessage, params json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.o.RequireInitialize && !s.initialized {
		s.replyLocked(id, nil, s.o.NotInitialized)
		return
	}
	var p struct {
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if json.Unmarshal(params, &p) != nil || p.Method == "" {
		s.replyLocked(id, nil, s.o.InvalidParams)
		return
	}
	fn := s.o.Methods[p.Method]
	if fn == nil {
		s.replyLocked(id, nil, NewError(CodeMethodNotFound, "Unknown method"))
		return
	}
	if s.inflight >= s.o.MaxInflight {
		s.replyLocked(id, nil, s.o.Busy)
		return
	}
	if len(p.Params) == 0 {
		p.Params = json.RawMessage("null")
	}
	s.inflight++
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		result, err := s.runMethod(fn, p.Params)
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

func (s *server) runMethod(fn MethodFunc, params json.RawMessage) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			result, err = nil, &panicError{r}
		}
	}()
	return fn(host.WithCaller(s.ctx, s.host), params)
}
