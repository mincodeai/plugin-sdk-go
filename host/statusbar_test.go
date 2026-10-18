package host

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestStatusBarHelpers(t *testing.T) {
	var method string
	var params json.RawMessage
	caller := CallerFunc(func(_ context.Context, m string, p any) (json.RawMessage, error) {
		method = m
		params, _ = json.Marshal(p)
		return json.RawMessage(`true`), nil
	})
	text, busy := "42%", true
	if err := UpdateStatusBarItem(context.Background(), caller, StatusBarUpdate{ID: "progress", Text: &text, Severity: "warning", Busy: &busy}); err != nil {
		t.Fatal(err)
	}
	if method != "host.statusBar.update" || string(params) != `{"id":"progress","text":"42%","severity":"warning","busy":true}` {
		t.Fatalf("update: %s %s", method, params)
	}
	if err := ResetStatusBar(context.Background(), caller, "progress"); err != nil || method != "host.statusBar.reset" || string(params) != `{"id":"progress"}` {
		t.Fatalf("reset one: %s %s %v", method, params, err)
	}
	if err := ResetStatusBar(context.Background(), caller, ""); err != nil || string(params) != `{}` {
		t.Fatalf("reset all: %s %v", params, err)
	}
	if err := UpdateStatusBarItem(context.Background(), caller, StatusBarUpdate{}); err == nil {
		t.Fatal("missing id accepted")
	}
	if err := UpdateStatusBarItem(context.Background(), nil, StatusBarUpdate{ID: "a"}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("nil caller: %v", err)
	}
	if err := ResetStatusBar(context.Background(), nil, ""); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("nil caller reset: %v", err)
	}
}
