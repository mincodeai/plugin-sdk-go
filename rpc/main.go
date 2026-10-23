package rpc

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/mincodeai/plugin-sdk-go/host"
)

// StrictDefaults is the Go-native family preset (nats-client, kafka-inspector,
// pprof-viewer, ...): strict JSON lines, "jsonrpc":"2.0" required, -32700
// replies for invalid lines, string/number ids only, a fatal 1 MiB line limit,
// invoke gated on initialize, params {action, payload string} and reverse
// request ids "<pluginID>-host-N".
func StrictDefaults(pluginID string) Options {
	return Options{
		MaxLineBytes:      1 << 20,
		OverlongFatal:     true,
		StrictJSON:        true,
		RequireVersion:    true,
		Invalid:           InvalidReply,
		JSONRPC2:          true,
		IDs:               IDStrict,
		Style:             StyleGo,
		MethodNotFound:    "Method not found",
		RequireInitialize: true,
		Payload:           PayloadString,
		Host:              hostOptions(pluginID + "-host-"),
	}
}

// NodeDefaults is the preset for plugins ported from the Node runtime
// (certificate-manager, hbuilder-simulator, todo, knowledge-base, excalidraw,
// devtools): lenient JSON, invalid lines reported on stderr, every present id
// answered, one request at a time, payload normalised to an object and
// reverse request ids "host-N".
func NodeDefaults() Options {
	return Options{
		MaxLineBytes:       1 << 20,
		Invalid:            InvalidStderr,
		InvalidMessage:     "Invalid plugin RPC request",
		JSONRPC2:           true,
		IDs:                IDAny,
		NonStringMethod:    MethodTypeUnknown,
		Style:              StyleGo,
		MethodNotFound:     "不支持的方法",
		MaxInflight:        1,
		Busy:               NewError(CodeBusy, "已有任务运行，请稍后重试"),
		Payload:            PayloadObject,
		UnknownAction:      func(string) *Error { return NewError(CodeMethodNotFound, "不支持的操作") },
		PayloadNotObject:   NewError(CodeInvalidParams, "参数必须是对象"),
		PayloadInvalidJSON: NewError(CodeInvalidParams, "Payload must contain valid JSON"),
		MapError:           PassHostErrors,
		Host:               hostOptions("host-"),
	}
}

// Main serves the process's standard input and output until EOF,
// plugin.shutdown or SIGINT/SIGTERM and exits with status 1 (after writing
// Options.FatalMessage) when the transport failed. Before serving it points
// os.Stdin at the null device and os.Stdout at os.Stderr, so libraries can
// neither consume nor corrupt protocol frames.
func Main(o Options) {
	in, out := takeStdio()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() { <-ctx.Done(); _ = in.Close() }()
	err := Serve(ctx, in, out, os.Stderr, o)
	stop()
	if err != nil {
		os.Exit(1)
	}
}

// takeStdio returns the process's standard input and output and points
// os.Stdin at the null device (nil if it cannot be opened, so reads fail)
// and os.Stdout at os.Stderr.
func takeStdio() (in, out *os.File) {
	in, out = os.Stdin, os.Stdout
	os.Stdin, _ = os.Open(os.DevNull)
	os.Stdout = os.Stderr
	return in, out
}

func hostOptions(prefix string) host.Options {
	return host.Options{IDPrefix: prefix}
}

// PassHostErrors is a MapError that answers host errors (*host.Error) with
// the host's own code and message, like the Node-runtime plugins that
// rethrew the host rejection; other errors become -32000 err.Error().
func PassHostErrors(err error) *Error {
	var he *host.Error
	if errors.As(err, &he) {
		return NewError(he.Code, he.Message)
	}
	return nil
}
