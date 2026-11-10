package host

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestPublishTestEvents(t *testing.T) {
	var method string
	var params json.RawMessage
	answer := json.RawMessage(`{"accepted":2,"dropped":0}`)
	var answerErr error
	caller := CallerFunc(func(_ context.Context, m string, p any) (json.RawMessage, error) {
		method = m
		params, _ = json.Marshal(p)
		return answer, answerErr
	})
	duration, expected := 12.0, ""
	result, err := PublishTestEvents(context.Background(), caller, "run-1", []TestRunEvent{
		{Type: TestEventTree, ID: "pkg.T#a", ParentID: "pkg.T", Label: "a()", Kind: TestKindTest},
		{Type: TestEventFailed, ID: "pkg.T#a", DurationMs: &duration, Message: "boom", Expected: &expected},
	})
	if err != nil || result.Accepted != 2 || method != "host.tests.publish" ||
		string(params) != `{"events":[{"type":"tree","id":"pkg.T#a","parentId":"pkg.T","label":"a()","kind":"test"},{"type":"failed","id":"pkg.T#a","durationMs":12,"message":"boom","expected":""}],"runId":"run-1"}` {
		t.Fatalf("publish = %+v %v %s %s", result, err, method, params)
	}
	if _, err := PublishTestEvents(context.Background(), caller, "run-1", nil); err != nil || string(params) != `{"events":[],"runId":"run-1"}` {
		t.Fatalf("nil events = %v %s", err, params)
	}
	answer, answerErr = nil, &Error{Code: -32601, Message: "host method not found"}
	if _, err := PublishTestEvents(context.Background(), caller, "run-1", nil); !errors.Is(err, ErrTestsUnsupported) {
		t.Fatalf("old host: %v", err)
	}
	answerErr = &Error{Code: CodeCapabilityMissing, Message: "the test run has ended"}
	if _, err := PublishTestEvents(context.Background(), caller, "run-1", nil); !IsCode(err, CodeCapabilityMissing) {
		t.Fatalf("ended run: %v", err)
	}
	if _, err := PublishTestEvents(context.Background(), caller, "", nil); err == nil {
		t.Fatal("empty run id accepted")
	}
	if _, err := PublishTestEvents(context.Background(), nil, "run-1", nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("nil caller: %v", err)
	}
	if EventTestRunEnded != "tests.runEnded" {
		t.Fatal("event name")
	}
}
