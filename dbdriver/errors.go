package dbdriver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// Kind classifies a driver error (JSON-RPC error data.kind).
type Kind string

// Error kinds of protocol v1.
const (
	KindDatabase    Kind = "database"
	KindConnection  Kind = "connection"
	KindCancelled   Kind = "cancelled"
	KindTimeout     Kind = "timeout"
	KindUnsupported Kind = "unsupported"
	KindInvalid     Kind = "invalid"
	KindBusy        Kind = "busy"
	// KindLimit reports a snapshotWrite whose snapshot rows do not fit one
	// reply; the transaction was rolled back and the host retries in
	// smaller batches.
	KindLimit Kind = "limit"
)

// Error is a classified driver error. Drivers return it (or wrap it) to pick
// the kind; any other error from Execute, Metadata, TableRows, Mutate or
// Destroy is KindDatabase, and from Connect KindConnection. Messages must not
// contain credentials; the Server also scrubs the connection's Secret values.
type Error struct {
	Kind    Kind
	Message string
	Err     error
}

func (e *Error) Error() string { return e.Message }

// Unwrap returns the underlying error.
func (e *Error) Unwrap() error { return e.Err }

// Errorf returns an *Error of kind with a formatted message.
func Errorf(kind Kind, format string, args ...any) *Error {
	return &Error{Kind: kind, Message: fmt.Sprintf(format, args...)}
}

// ErrUnsupported is returned for methods the Conn does not implement.
var ErrUnsupported = &Error{Kind: KindUnsupported, Message: "operation not supported by this driver"}

// Messages the Server uses for context errors.
const (
	msgCancelled = "query cancelled"
	msgTimeout   = "query timed out"
	msgClosed    = "connection closed"
)

// classify turns any error into an *Error. ctx decides cancellation and
// timeouts over whatever the driver reported.
func classify(ctx context.Context, err error, def Kind) *Error {
	if ctx != nil {
		switch {
		case errors.Is(context.Cause(ctx), errHandleClosed):
			return &Error{Kind: KindCancelled, Message: msgClosed}
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			return &Error{Kind: KindTimeout, Message: msgTimeout}
		case ctx.Err() != nil:
			return &Error{Kind: KindCancelled, Message: msgCancelled}
		}
	}
	var e *Error
	if errors.As(err, &e) {
		if e.Kind == "" {
			return &Error{Kind: def, Message: e.Message}
		}
		return e
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return &Error{Kind: KindTimeout, Message: msgTimeout}
	case errors.Is(err, context.Canceled):
		return &Error{Kind: KindCancelled, Message: msgCancelled}
	}
	return &Error{Kind: def, Message: err.Error()}
}

var errHandleClosed = errors.New("dbdriver: handle closed")

// rpcError renders e as the JSON-RPC error of the reply.
func rpcError(e *Error, scrub func(string) string) *rpc.Error {
	code := rpc.CodeServerError
	switch e.Kind {
	case KindInvalid:
		code = rpc.CodeInvalidParams
	case KindBusy:
		code = rpc.CodeBusy
	}
	msg := e.Message
	if scrub != nil {
		msg = scrub(msg)
	}
	msg = clipRunes(msg, maxErrorRunes)
	if msg == "" {
		msg = string(e.Kind)
	}
	data, _ := json.Marshal(struct {
		Kind Kind `json:"kind"`
	}{e.Kind})
	return &rpc.Error{Code: code, Message: msg, Data: data}
}

func invalidf(format string, args ...any) *rpc.Error {
	return rpcError(Errorf(KindInvalid, format, args...), nil)
}
