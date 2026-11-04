package host

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// jdtlsCommandTimeout is the host's limit for one jdtls command.
const jdtlsCommandTimeout = 60 * time.Second

// ErrJdtlsUnsupported is returned by ExecuteJdtlsCommand when the host has no
// host.jdtls.executeCommand (hosts before 0.3.0) or the plugin declares no
// contributes.jdtlsBundles (-32601).
var ErrJdtlsUnsupported = errors.New("host does not support host.jdtls.executeCommand")

// ExecuteJdtlsCommand calls host.jdtls.executeCommand (host 0.3.0; needs the
// shell capability and contributes.jdtlsBundles): the host sends
// workspace/executeCommand with command and args to the built-in Java
// language server (jdtls) running for the absolute project folder root and
// returns its raw JSON result ("null" when it returned none). Only commands
// listed in the plugin's own jdtlsBundles entries are allowed (-32001
// otherwise), and only after the user confirmed the plugin's jars and jdtls
// was (re)started with them; a jdtls that is not running or was started
// without the jars answers -32000 with a message starting "unavailable:".
// The host waits at most 60 s and refuses results above 1 MiB (-32603).
func ExecuteJdtlsCommand(ctx context.Context, c Caller, root, command string, args ...any) (json.RawMessage, error) {
	if c == nil {
		return nil, ErrUnavailable
	}
	if root == "" || command == "" {
		return nil, errors.New("jdtls command requires a project root and a command")
	}
	if args == nil {
		args = []any{}
	}
	params := map[string]any{"root": root, "command": command, "arguments": args}
	raw, err := c.Call(WithCallTimeout(ctx, jdtlsCommandTimeout+replyMargin), "host.jdtls.executeCommand", params)
	if IsCode(err, -32601) {
		return nil, ErrJdtlsUnsupported
	}
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		raw = json.RawMessage("null")
	}
	return raw, nil
}
