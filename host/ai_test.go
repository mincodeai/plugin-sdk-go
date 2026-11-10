package host

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestAITurnHelpers(t *testing.T) {
	var method string
	var params json.RawMessage
	caller := CallerFunc(func(_ context.Context, m string, p any) (json.RawMessage, error) {
		method = m
		params, _ = json.Marshal(p)
		return json.RawMessage(`{"id":"aiturn-1","status":"awaiting_approval","done":false,"text":"partial","activity":"waiting","conversationId":"conversation-1"}`), nil
	})
	state, err := StartAITurn(context.Background(), caller, AITurnRequest{ConversationKey: "feishu:oc_1", Title: "群", Prompt: "hi"})
	if err != nil || method != "host.ai.turn.start" || state.Status != AITurnAwaitingApproval || state.Text != "partial" || state.ConversationID != "conversation-1" {
		t.Fatalf("start: %s %+v %v", method, state, err)
	}
	if string(params) != `{"conversationKey":"feishu:oc_1","title":"群","prompt":"hi","allowedPlugins":null}` {
		t.Fatalf("start params: %s", params)
	}
	if _, err := CancelAITurn(context.Background(), caller, "aiturn-1"); err != nil || method != "host.ai.turn.cancel" || string(params) != `{"id":"aiturn-1"}` {
		t.Fatalf("cancel: %s %s %v", method, params, err)
	}
	if _, err := PollAITurn(context.Background(), nil, "x"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("nil caller: %v", err)
	}
}

func TestAITurnRestrictionsAndPluginList(t *testing.T) {
	var params json.RawMessage
	caller := CallerFunc(func(_ context.Context, m string, p any) (json.RawMessage, error) {
		params, _ = json.Marshal(p)
		if m == "host.ai.plugins" {
			return json.RawMessage(`[{"id":"log-explorer","name":"Log","enabled":true,"actions":["query"]}]`), nil
		}
		return json.RawMessage(`{"id":"aiturn-1","status":"pending"}`), nil
	})
	if _, err := StartAITurn(context.Background(), caller, AITurnRequest{ConversationKey: "k", Prompt: "hi", Instructions: "简洁", AllowedPlugins: []string{}}); err != nil {
		t.Fatal(err)
	}
	if string(params) != `{"conversationKey":"k","prompt":"hi","instructions":"简洁","allowedPlugins":[]}` {
		t.Fatalf("an empty allow-list must be sent as []: %s", params)
	}
	plugins, err := ListAIPlugins(context.Background(), caller)
	if err != nil || len(plugins) != 1 || plugins[0].ID != "log-explorer" || plugins[0].Actions[0] != "query" {
		t.Fatalf("plugins: %+v %v", plugins, err)
	}
}

func TestAIGrantsAndApproval(t *testing.T) {
	var method string
	var params json.RawMessage
	caller := CallerFunc(func(_ context.Context, m string, p any) (json.RawMessage, error) {
		method = m
		params, _ = json.Marshal(p)
		if m == "host.ai.grants" {
			return json.RawMessage(`{"chatApproval":true,"workspaceAccess":false}`), nil
		}
		return json.RawMessage(`{"id":"aiturn-1","status":"running"}`), nil
	})
	grants, err := AIGrantsFor(context.Background(), caller)
	if err != nil || !grants.ChatApproval || grants.WorkspaceAccess {
		t.Fatalf("grants %+v %v", grants, err)
	}
	if _, err := ApproveAITurn(context.Background(), caller, "aiturn-1", "approval-1", true); err != nil || method != "host.ai.turn.approve" ||
		string(params) != `{"approvalId":"approval-1","approved":true,"id":"aiturn-1"}` {
		t.Fatalf("approve %s %s %v", method, params, err)
	}
	if _, err := StartAITurn(context.Background(), caller, AITurnRequest{ConversationKey: "k", Prompt: "hi", MaxRisk: "read", ChatApproval: true}); err != nil ||
		string(params) != `{"conversationKey":"k","prompt":"hi","allowedPlugins":null,"maxRisk":"read","chatApproval":true}` {
		t.Fatalf("start params %s %v", params, err)
	}
	if _, err := StartAITurn(context.Background(), caller, AITurnRequest{ConversationKey: "k", Prompt: "hi", History: AITurnHistoryNone}); err != nil ||
		string(params) != `{"conversationKey":"k","prompt":"hi","allowedPlugins":null,"history":"none"}` {
		t.Fatalf("history none params %s %v", params, err)
	}
	method = ""
	for _, history := range []string{"all", "None", " none"} {
		if _, err := StartAITurn(context.Background(), caller, AITurnRequest{ConversationKey: "k", Prompt: "hi", History: history}); err == nil || method != "" {
			t.Fatalf("history %q must be refused before calling the host: %v %s", history, err, method)
		}
	}
}
