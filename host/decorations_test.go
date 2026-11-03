package host

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestRefreshDecorations(t *testing.T) {
	var method string
	var params json.RawMessage
	answer := json.RawMessage(`{"scheduled":true}`)
	var answerErr error
	caller := CallerFunc(func(_ context.Context, m string, p any) (json.RawMessage, error) {
		method = m
		params, _ = json.Marshal(p)
		return answer, answerErr
	})
	scheduled, err := RefreshDecorations(context.Background(), caller)
	if err != nil || !scheduled || method != "host.decorations.refresh" || string(params) != `{}` {
		t.Fatalf("refresh = %v %v %s %s", scheduled, err, method, params)
	}
	answer = json.RawMessage(`{"scheduled":false}`)
	if scheduled, err := RefreshDecorations(context.Background(), caller); err != nil || scheduled {
		t.Fatalf("rate limited = %v %v", scheduled, err)
	}
	// Hosts before 0.3.0 (or a plugin without fileDecorations) answer -32601.
	answer, answerErr = nil, &Error{Code: -32601, Message: "host method not found"}
	if scheduled, err := RefreshDecorations(context.Background(), caller); err != nil || scheduled {
		t.Fatalf("old host = %v %v", scheduled, err)
	}
	answerErr = &Error{Code: CodeCapabilityMissing, Message: "workspace.read capability not granted"}
	if _, err := RefreshDecorations(context.Background(), caller); err == nil {
		t.Fatal("capability error swallowed")
	}
	answer, answerErr = json.RawMessage(`[]`), nil
	if _, err := RefreshDecorations(context.Background(), caller); err == nil {
		t.Fatal("invalid result accepted")
	}
	if _, err := RefreshDecorations(context.Background(), nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("nil caller: %v", err)
	}
}
