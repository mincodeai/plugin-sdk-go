package host

import (
	"context"
	"encoding/json"
	"testing"
)

func TestNotifyAndClipboardHelpers(t *testing.T) {
	var method string
	var params json.RawMessage
	caller := CallerFunc(func(_ context.Context, m string, p any) (json.RawMessage, error) {
		method = m
		params, _ = json.Marshal(p)
		if m == "host.clipboard.read" {
			return json.RawMessage(`{"changed":true,"seq":9,"text":"hi"}`), nil
		}
		return json.RawMessage(`{}`), nil
	})
	if err := Notify(context.Background(), caller, Notification{Title: "专注结束", Level: "success", System: true}); err != nil ||
		method != "host.notify" || string(params) != `{"title":"专注结束","level":"success","system":true}` {
		t.Fatalf("notify %s %s %v", method, params, err)
	}
	got, err := ReadClipboard(context.Background(), caller, 8)
	if err != nil || !got.Changed || got.Seq != 9 || got.Text != "hi" || string(params) != `{"since":8}` {
		t.Fatalf("read %+v %s %v", got, params, err)
	}
	if err := WriteClipboard(context.Background(), caller, "x"); err != nil || method != "host.clipboard.write" {
		t.Fatalf("write %s %v", method, err)
	}
	if err := Notify(context.Background(), nil, Notification{Title: "x"}); err != ErrUnavailable {
		t.Fatalf("nil caller: %v", err)
	}
}

func TestNotifyActions(t *testing.T) {
	var params json.RawMessage
	reply := `{"delivered":true,"actions":1}`
	caller := CallerFunc(func(_ context.Context, _ string, p any) (json.RawMessage, error) {
		params, _ = json.Marshal(p)
		return json.RawMessage(reply), nil
	})
	result, err := NotifyWithResult(context.Background(), caller, Notification{
		Title: "同步失败", Actions: []NotificationAction{{ID: "retry", Title: "重试", Command: "sync.retry"}},
	})
	if err != nil || string(params) != `{"title":"同步失败","actions":[{"id":"retry","title":"重试","command":"sync.retry"}]}` {
		t.Fatalf("params %s %v", params, err)
	}
	if !result.Delivered || result.Actions == nil || *result.Actions != 1 {
		t.Fatalf("result %+v", result)
	}
	reply = `{"delivered":true}`
	result, err = NotifyWithResult(context.Background(), caller, Notification{Title: "x"})
	if err != nil || !result.Delivered || result.Actions != nil || string(params) != `{"title":"x"}` {
		t.Fatalf("old host %+v %s %v", result, params, err)
	}
	if _, err := NotifyWithResult(context.Background(), nil, Notification{Title: "x"}); err != ErrUnavailable {
		t.Fatalf("nil caller: %v", err)
	}
}
