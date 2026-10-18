package host

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompleteAITurnStreamsUntilDone(t *testing.T) {
	var polls atomic.Int32
	caller := CallerFunc(func(_ context.Context, m string, _ any) (json.RawMessage, error) {
		switch m {
		case "host.ai.turn.start":
			return json.RawMessage(`{"id":"t1","status":"running","text":"He"}`), nil
		case "host.ai.turn.poll":
			if polls.Add(1) == 1 {
				return json.RawMessage(`{"id":"t1","status":"running","text":"Hello"}`), nil
			}
			return json.RawMessage(`{"id":"t1","status":"completed","done":true,"text":"Hello!"}`), nil
		}
		return nil, errors.New("unexpected " + m)
	})
	var seen []string
	text, err := CompleteAITurn(context.Background(), caller, AITurnRequest{ConversationKey: "k", Prompt: "p"}, time.Millisecond, func(s string) { seen = append(seen, s) })
	if err != nil || text != "Hello!" || len(seen) != 3 || seen[1] != "Hello" {
		t.Fatalf("text=%q err=%v seen=%v", text, err, seen)
	}
}

func TestCompleteAITurnFailureAndCancel(t *testing.T) {
	failed := CallerFunc(func(_ context.Context, m string, _ any) (json.RawMessage, error) {
		return json.RawMessage(`{"id":"t1","status":"failed","done":true,"error":" boom "}`), nil
	})
	_, err := CompleteAITurn(context.Background(), failed, AITurnRequest{ConversationKey: "k", Prompt: "p"}, time.Millisecond, nil)
	var te *AITurnError
	if !errors.As(err, &te) || te.Kind != AIErrFailed || te.Message != "boom" || ClassifyAIError(err) != AIErrFailed {
		t.Fatalf("failed: %v", err)
	}

	cancelled := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	running := CallerFunc(func(_ context.Context, m string, _ any) (json.RawMessage, error) {
		if m == "host.ai.turn.cancel" {
			cancelled <- struct{}{}
		}
		if m == "host.ai.turn.poll" {
			cancel()
		}
		return json.RawMessage(`{"id":"t1","status":"running"}`), nil
	})
	_, err = CompleteAITurn(ctx, running, AITurnRequest{ConversationKey: "k", Prompt: "p"}, time.Millisecond, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("turn was not cancelled on the host")
	}
}

func TestClassifyAIError(t *testing.T) {
	cases := map[AIErrorKind]error{
		AIErrRateLimited:  &Error{Code: -32000, Message: "hourly AI turn limit reached for this plugin"},
		AIErrBusy:         &Error{Code: -32000, Message: "too many AI turns are running for this plugin"},
		AIErrNoCapability: &Error{Code: CodeCapabilityMissing},
		AIErrUnsupported:  &Error{Code: -32601},
		AIErrNone:         errors.New("other"),
	}
	for want, err := range cases {
		if got := ClassifyAIError(err); got != want {
			t.Errorf("%v: got %q want %q", err, got, want)
		}
	}
	if got := AIErrorMessage(&Error{Code: -32000, Message: "MinCode AI 助手未就绪"}); got != "MinCode AI 助手未就绪" {
		t.Errorf("host message: %q", got)
	}
	if got := AIErrorMessage(&AITurnError{Kind: AIErrFailed, Message: "x"}); got != "AI 回答失败：x" {
		t.Errorf("failed message: %q", got)
	}
	if got := AIErrorMessage(context.Canceled); got != "已取消" {
		t.Errorf("cancel message: %q", got)
	}
}
