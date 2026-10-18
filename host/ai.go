package host

import (
	"context"
	"encoding/json"
)

// AI turns (manifest capability "ai"): ask the desktop AI assistant to answer
// a message. The assistant runs the turn with its own model, tools, approvals
// and usage accounting in a conversation reserved for the plugin; the plugin
// polls for the streamed text and relays it (for example to a team chat).

// AITurnRequest starts one turn. ConversationKey groups turns into one
// assistant conversation (e.g. "feishu:oc_123"); pass the ConversationID from
// an earlier AITurnState to keep using the same conversation.
type AITurnRequest struct {
	ConversationKey string `json:"conversationKey"`
	ConversationID  string `json:"conversationId,omitempty"`
	// Title names the conversation in the assistant (≤ 80 characters).
	Title string `json:"title,omitempty"`
	// Sender is shown to the model as the author of the message (≤ 80 characters).
	Sender string `json:"sender,omitempty"`
	// Prompt is the message to answer (≤ 16 KiB).
	Prompt string `json:"prompt"`
	// Instructions from the plugin's own configuration (a bot persona, answer
	// rules; ≤ 4 KiB) are added to the system prompt, above the relayed message.
	Instructions string `json:"instructions,omitempty"`
	// AllowedPlugins limits which plugins the assistant may list, describe and
	// invoke for this turn. nil means no restriction; an empty slice allows none.
	AllowedPlugins []string `json:"allowedPlugins"`
	// MaxRisk caps tool risk for the turn: "read", "network", "write" or
	// "dangerous" ("" = no cap). Riskier calls are refused without asking.
	MaxRisk string `json:"maxRisk,omitempty"`
	// ChatApproval exposes the turn's approvals (AITurnState.Approval) so the
	// plugin can decide them with ApproveAITurn. Requires the desktop grant.
	ChatApproval bool `json:"chatApproval,omitempty"`
	// WorkspaceAccess runs the turn like a local conversation (active workspace
	// project, all tools, still with approvals). Requires the desktop grant.
	WorkspaceAccess bool `json:"workspaceAccess,omitempty"`
}

// AITurnApproval is a pending tool approval of a ChatApproval turn.
type AITurnApproval struct {
	ID      string `json:"id"`
	Summary string `json:"summary"`
	Risk    string `json:"risk"`
	Tool    string `json:"tool"`
}

// AIGrants are what the desktop user allowed this plugin in MinCode.
type AIGrants struct {
	ChatApproval    bool `json:"chatApproval"`
	WorkspaceAccess bool `json:"workspaceAccess"`
}

// AITurnStatus values: pending, running, awaiting_approval, completed, failed, cancelled.
type AITurnStatus string

const (
	AITurnPending          AITurnStatus = "pending"
	AITurnRunning          AITurnStatus = "running"
	AITurnAwaitingApproval AITurnStatus = "awaiting_approval"
	AITurnCompleted        AITurnStatus = "completed"
	AITurnFailed           AITurnStatus = "failed"
	AITurnCancelled        AITurnStatus = "cancelled"
)

// AITurnState is a snapshot of a turn. Text is the full reply so far (not a
// delta); Done is true once Status is completed, failed or cancelled.
type AITurnState struct {
	ID             string       `json:"id"`
	Status         AITurnStatus `json:"status"`
	Done           bool         `json:"done"`
	Text           string       `json:"text"`
	Activity       string       `json:"activity,omitempty"`
	Error          string       `json:"error,omitempty"`
	ConversationID string       `json:"conversationId,omitempty"`
	// Approval is set while a ChatApproval turn waits for a decision.
	Approval *AITurnApproval `json:"approval,omitempty"`
}

// AIPlugin is a plugin whose actions the assistant can call (see ListAIPlugins).
type AIPlugin struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Enabled     bool     `json:"enabled"`
	Actions     []string `json:"actions"`
}

// ListAIPlugins lists installed plugins that expose AI-callable actions, other
// than the caller, e.g. to let a user choose AITurnRequest.AllowedPlugins.
func ListAIPlugins(ctx context.Context, c Caller) ([]AIPlugin, error) {
	if c == nil {
		return nil, ErrUnavailable
	}
	raw, err := c.Call(ctx, "host.ai.plugins", map[string]any{})
	if err != nil {
		return nil, err
	}
	var plugins []AIPlugin
	if err := json.Unmarshal(raw, &plugins); err != nil {
		return nil, err
	}
	return plugins, nil
}

// AIGrantsFor reports the desktop grants of the calling plugin.
func AIGrantsFor(ctx context.Context, c Caller) (AIGrants, error) {
	if c == nil {
		return AIGrants{}, ErrUnavailable
	}
	raw, err := c.Call(ctx, "host.ai.grants", map[string]any{})
	if err != nil {
		return AIGrants{}, err
	}
	var grants AIGrants
	err = json.Unmarshal(raw, &grants)
	return grants, err
}

// ApproveAITurn decides the pending approval of a ChatApproval turn.
func ApproveAITurn(ctx context.Context, c Caller, id, approvalID string, approved bool) (AITurnState, error) {
	return aiTurnCall(ctx, c, "host.ai.turn.approve", map[string]any{"id": id, "approvalId": approvalID, "approved": approved})
}

// StartAITurn submits a turn and returns its initial state.
func StartAITurn(ctx context.Context, c Caller, request AITurnRequest) (AITurnState, error) {
	return aiTurnCall(ctx, c, "host.ai.turn.start", request)
}

// PollAITurn returns the latest state of a turn this plugin started.
func PollAITurn(ctx context.Context, c Caller, id string) (AITurnState, error) {
	return aiTurnCall(ctx, c, "host.ai.turn.poll", map[string]string{"id": id})
}

// CancelAITurn stops a turn this plugin started.
func CancelAITurn(ctx context.Context, c Caller, id string) (AITurnState, error) {
	return aiTurnCall(ctx, c, "host.ai.turn.cancel", map[string]string{"id": id})
}

func aiTurnCall(ctx context.Context, c Caller, method string, params any) (AITurnState, error) {
	if c == nil {
		return AITurnState{}, ErrUnavailable
	}
	raw, err := c.Call(ctx, method, params)
	if err != nil {
		return AITurnState{}, err
	}
	var state AITurnState
	if err := json.Unmarshal(raw, &state); err != nil {
		return AITurnState{}, err
	}
	return state, nil
}
