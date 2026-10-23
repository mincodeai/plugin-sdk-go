package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/mincodeai/plugin-sdk-go/host"
)

// Free-form view messages, like VS Code's webview postMessage: the plugin's
// views send plugin.ui.message requests (handled by Options.OnUIMessage), and
// PostUIMessage sends host.ui.postMessage notifications to the views. Neither
// direction passes the host's risk confirmation; keep writes and dangerous
// operations in actions.

// MaxUIMessageBytes bounds one view message as encoded JSON.
const MaxUIMessageBytes = 256 * 1024

var (
	// ErrUIMessageUnavailable is returned by PostUIMessage outside a server context.
	ErrUIMessageUnavailable = errors.New("rpc: PostUIMessage needs a context from the server")
	// ErrUIMessageInvalid rejects a nil message or an invalid view ID.
	ErrUIMessageInvalid = errors.New("rpc: view message must be a non-null JSON value and viewId a manifest view ID")
	// ErrUIMessageTooLarge rejects a message above MaxUIMessageBytes.
	ErrUIMessageTooLarge = fmt.Errorf("rpc: view message exceeds %d bytes", MaxUIMessageBytes)
)

var uiViewIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

type uiMessage struct {
	viewID  string
	message json.RawMessage
}

// PostUIMessage sends message to the plugin's open views (host.ui.postMessage;
// needs the ui capability). An empty viewID reaches every open view; otherwise
// only views with that manifest view ID. Views receive messages in the order
// they were posted. ctx must come from the server (handler, task, OnInitialize,
// OnUIMessage, OnBinary or OnEvent context).
func PostUIMessage(ctx context.Context, message any, viewID string) error {
	s, _ := ctx.Value(binaryKey{}).(*server)
	if s == nil {
		return ErrUIMessageUnavailable
	}
	if viewID != "" && !uiViewIDPattern.MatchString(viewID) {
		return ErrUIMessageInvalid
	}
	encoded, err := json.Marshal(message)
	if err != nil || string(encoded) == "null" {
		return ErrUIMessageInvalid
	}
	if len(encoded) > MaxUIMessageBytes {
		return ErrUIMessageTooLarge
	}
	params := struct {
		Message json.RawMessage `json:"message"`
		ViewID  string          `json:"viewId,omitempty"`
	}{encoded, viewID}
	line, err := json.Marshal(struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  any    `json:"params"`
	}{"2.0", "host.ui.postMessage", params})
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.replyStopped {
		return host.ErrUnavailable
	}
	return s.writeLocked(line)
}

// uiMessageRequest acknowledges plugin.ui.message and queues it for
// OnUIMessage, which runs on one goroutine in arrival order. A full queue
// answers busy instead of blocking the reader. Caller holds no lock.
func (s *server) uiMessageRequest(id json.RawMessage, params json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.o.OnUIMessage == nil {
		s.replyLocked(id, nil, NewError(CodeMethodNotFound, s.o.MethodNotFound))
		return
	}
	var p struct {
		ViewID  string          `json:"viewId"`
		Message json.RawMessage `json:"message"`
	}
	if err := json.Unmarshal(params, &p); err != nil || len(p.Message) == 0 || string(p.Message) == "null" {
		s.replyLocked(id, nil, NewError(CodeInvalidParams, "invalid view message params"))
		return
	}
	s.uiMessagesOnce.Do(func() {
		s.uiMessages = make(chan uiMessage, 64)
		go s.uiMessageLoop()
	})
	select {
	case s.uiMessages <- uiMessage{p.ViewID, p.Message}:
		s.replyLocked(id, json.RawMessage(`{}`), nil)
	default:
		s.replyLocked(id, nil, NewError(CodeBusy, "view message queue full; try again later"))
	}
}

func (s *server) uiMessageLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case m := <-s.uiMessages:
			s.deliverUIMessage(m)
		}
	}
}

func (s *server) deliverUIMessage(m uiMessage) {
	defer func() {
		if r := recover(); r != nil && s.errOut != nil {
			_, _ = fmt.Fprintf(s.errOut, "plugin view message handler panicked: %v\n", r)
		}
	}()
	s.o.OnUIMessage(host.WithCaller(s.ctx, s.host), m.viewID, m.message)
}
