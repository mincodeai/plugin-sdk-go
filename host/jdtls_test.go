package host

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestExecuteJdtlsCommand(t *testing.T) {
	var method string
	var params json.RawMessage
	var wait time.Duration
	answer := json.RawMessage(`{"classpaths":["/a"]}`)
	var answerErr error
	caller := CallerFunc(func(ctx context.Context, m string, p any) (json.RawMessage, error) {
		method = m
		params, _ = json.Marshal(p)
		wait, _ = ctx.Value(callTimeoutKey{}).(time.Duration)
		return answer, answerErr
	})
	result, err := ExecuteJdtlsCommand(context.Background(), caller, "/work/app", "vscode.java.resolveClasspath", "com.example.Main", nil)
	if err != nil || string(result) != `{"classpaths":["/a"]}` || method != "host.jdtls.executeCommand" ||
		string(params) != `{"arguments":["com.example.Main",null],"command":"vscode.java.resolveClasspath","root":"/work/app"}` {
		t.Fatalf("execute = %s %v %s %s", result, err, method, params)
	}
	// The host waits up to 60 s for jdtls; the call waits a little longer.
	if wait <= 60*time.Second {
		t.Fatalf("call timeout = %v", wait)
	}
	// No arguments sends an empty list.
	if _, err := ExecuteJdtlsCommand(context.Background(), caller, "/work/app", "vscode.java.resolveJavaExecutable"); err != nil || string(params) != `{"arguments":[],"command":"vscode.java.resolveJavaExecutable","root":"/work/app"}` {
		t.Fatalf("no args = %v %s", err, params)
	}
	// A host without the method, or a plugin without jdtlsBundles, answers -32601.
	answer, answerErr = nil, &Error{Code: -32601, Message: "host method not found"}
	if _, err := ExecuteJdtlsCommand(context.Background(), caller, "/work/app", "x"); !errors.Is(err, ErrJdtlsUnsupported) {
		t.Fatalf("old host: %v", err)
	}
	answerErr = &Error{Code: CodeCapabilityMissing, Message: "command is not declared by this plugin"}
	if _, err := ExecuteJdtlsCommand(context.Background(), caller, "/work/app", "x"); !IsCode(err, CodeCapabilityMissing) {
		t.Fatalf("undeclared: %v", err)
	}
	if _, err := ExecuteJdtlsCommand(context.Background(), caller, "", "x"); err == nil {
		t.Fatal("empty root accepted")
	}
	if _, err := ExecuteJdtlsCommand(context.Background(), caller, "/work/app", ""); err == nil {
		t.Fatal("empty command accepted")
	}
	if _, err := ExecuteJdtlsCommand(context.Background(), nil, "/work/app", "x"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("nil caller: %v", err)
	}
}
