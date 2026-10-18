package host

import (
	"context"
	"encoding/json"
	"errors"
)

// CallablePlugin is another installed plugin whose AI-callable actions this
// plugin may call with InvokePlugin. Granted reports whether the user chose
// "always allow" for its read-only actions.
type CallablePlugin struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Enabled     bool     `json:"enabled"`
	Actions     []string `json:"actions"`
	Granted     bool     `json:"granted"`
}

// ListPlugins calls host.plugins.list (plugins capability).
func ListPlugins(ctx context.Context, c Caller) ([]CallablePlugin, error) {
	if c == nil {
		return nil, ErrUnavailable
	}
	raw, err := c.Call(ctx, "host.plugins.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var result struct {
		Plugins []CallablePlugin `json:"plugins"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, errors.New("host returned an invalid plugin list")
	}
	return result.Plugins, nil
}

// PluginCall names an AI-callable action of another plugin. Payload must
// match the action's inputSchema (nil sends {}). TimeoutMs bounds the user's
// approval when one is needed (0: 25 s; 5000-300000).
type PluginCall struct {
	PluginID  string
	Action    string
	Payload   any
	TimeoutMs int
}

// InvokePlugin calls host.plugins.invoke (plugins capability). The user is
// asked the first time this plugin calls that plugin, and for every action
// that is not read-risk; a declined call returns a host error with code
// -32003. The host-call timeout covers the approval plus the action's own
// 30 s.
func InvokePlugin(ctx context.Context, c Caller, call PluginCall) (json.RawMessage, error) {
	if c == nil {
		return nil, ErrUnavailable
	}
	if call.PluginID == "" || call.Action == "" {
		return nil, errors.New("plugin call requires a plugin id and an action")
	}
	payload := call.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	params := map[string]any{"pluginId": call.PluginID, "action": call.Action, "payload": payload}
	if call.TimeoutMs > 0 {
		params["timeoutMs"] = call.TimeoutMs
	}
	wait := promptWait(call.TimeoutMs) + pluginInvokeTimeout + replyMargin
	raw, err := c.Call(WithCallTimeout(ctx, wait), "host.plugins.invoke", params)
	if err != nil {
		return nil, err
	}
	var result struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, errors.New("host returned an invalid plugin call result")
	}
	return result.Result, nil
}
