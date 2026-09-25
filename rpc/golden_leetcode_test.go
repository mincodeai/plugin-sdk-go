package rpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// leetcode-cn preset (recorded from the pre-SDK binary): the former loop's
// request struct is reproduced by DecodeEnvelope, -32700 replies carry no id
// (InvalidOmitID), an empty payload reaches the handler as-is
// (KeepEmptyPayload) and every handler error is -32602 err.Error(). Tasks are
// enabled; a host that does not negotiate them sees "method not found".
func init() {
	extraPresets["leetcode-cn"] = func(*testing.T) rpc.Options {
		o := rpc.StrictDefaults("leetcode-cn")
		o.FatalMessage = "read JSON-RPC request: bufio.Scanner: token too long"
		o.StrictJSON = false
		o.RequireVersion = false
		o.DecodeEnvelope = leetcodeEnvelope
		o.InvalidMessage = "invalid JSON-RPC request"
		o.InvalidOmitID = true
		o.IDs = rpc.IDAny
		o.MethodNotFound = "method not found"
		o.TasksNotNegotiated = "method not found"
		o.RequireInitialize = false
		o.MaxInflight = 32
		o.InvalidParams = rpc.NewError(rpc.CodeInvalidParams, "invalid invoke parameters")
		o.KeepEmptyPayload = true
		o.MapError = func(err error) *rpc.Error { return rpc.NewError(rpc.CodeInvalidParams, err.Error()) }
		o.Tasks = &rpc.TaskOptions{MaxRunning: 2}
		o.Handler = leetcodeStub
		return o
	}
}

func leetcodeEnvelope(line []byte) (map[string]json.RawMessage, bool) {
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	var request struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id,omitempty"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params,omitempty"`
	}
	if decoder.Decode(&request) != nil || request.JSONRPC != "2.0" || len(request.ID) == 0 || strings.TrimSpace(request.Method) == "" {
		return nil, false
	}
	method, _ := json.Marshal(request.Method)
	env := map[string]json.RawMessage{"jsonrpc": json.RawMessage(`"2.0"`), "id": request.ID, "method": method}
	if request.Params != nil {
		env["params"] = request.Params
	}
	return env, true
}

// leetcodeStub answers the recorded actions without network access: payloads
// are decoded strictly per action and the recorded validation outcome is
// looked up by payload.
func leetcodeStub(_ context.Context, action string, payload json.RawMessage) (any, error) {
	type judge struct{ TitleSlug, Lang, Code, DataInput string }
	targets := map[string]func() any{
		"leetcode.login":         func() any { return &struct{ Session, CSRFToken, CookieHeader string }{} },
		"leetcode.login-browser": func() any { return &struct{ Browser string }{} },
		"leetcode.list": func() any {
			return &struct {
				Skip, Limit                 int
				Difficulty, Keyword, Status string
			}{}
		},
		"leetcode.detail": func() any { return &struct{ TitleSlug string }{} },
		"leetcode.run":    func() any { return &judge{} },
		"leetcode.submit": func() any { return &judge{} },
	}
	switch action {
	case "leetcode.status", "leetcode.logout":
		return json.RawMessage(`{"loggedIn":false}`), nil
	}
	target, ok := targets[action]
	if !ok {
		return nil, errors.New("未知动作：" + action)
	}
	raw := string(payload)
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if strings.TrimSpace(raw) == "" || decoder.Decode(target()) != nil {
		return nil, errors.New("payload must be a JSON object")
	}
	switch {
	case strings.Contains(raw, "Bad_Slug"), raw == "{}":
		return nil, errors.New("titleSlug 格式不正确")
	case strings.Contains(raw, `"keyword"`):
		return nil, errors.New("搜索关键词最长 80 个字符")
	case strings.Contains(raw, `c++\";`):
		return nil, errors.New("lang 格式不正确")
	case strings.Contains(raw, `"code":"  "`):
		return nil, errors.New("代码不能为空")
	case strings.Contains(raw, `"dataInput"`):
		return nil, errors.New("自定义输入超过 16384 字节限制")
	case strings.Contains(raw, `"session":"a b"`):
		return nil, errors.New("LEETCODE_SESSION 格式不正确：应为不含空格与分号的 Cookie 值")
	case strings.Contains(raw, `"csrfToken"`):
		return nil, errors.New("csrftoken 格式不正确：应为不含空格与分号的 Cookie 值")
	case strings.Contains(raw, "other=1"):
		return nil, errors.New("Cookie 中未找到 LEETCODE_SESSION：请复制已登录 leetcode.cn 请求的 Cookie 请求头")
	case strings.Contains(raw, "LEETCODE_SESSION=two"):
		return nil, errors.New("Cookie 中包含重复的 LEETCODE_SESSION")
	case action == "leetcode.run":
		return nil, errors.New("请先登录后再运行代码")
	case action == "leetcode.submit":
		return nil, errors.New("请先登录后再提交代码")
	}
	return nil, errors.New("unexpected payload " + raw)
}

func TestDecodeEnvelopeInvalidOmitIDKeepEmptyPayload(t *testing.T) {
	o := rpc.StrictDefaults("x")
	o.StrictJSON = false
	o.RequireVersion = false
	o.RequireInitialize = false
	o.IDs = rpc.IDAny
	o.InvalidMessage = "bad"
	o.InvalidOmitID = true
	o.KeepEmptyPayload = true
	o.DecodeEnvelope = leetcodeEnvelope
	o.Handler = func(_ context.Context, action string, payload json.RawMessage) (any, error) {
		return map[string]string{"action": action, "payload": string(payload)}, nil
	}
	p := startPipe(t, o)
	for _, c := range []struct{ in, want string }{
		// DecodeEnvelope rejects unknown members; the reply has no id member.
		{`{"jsonrpc":"2.0","id":1,"method":"plugin.ping","extra":1}`, `{"jsonrpc":"2.0","error":{"code":-32700,"message":"bad"}}`},
		{`{"jsonrpc":"2.0","id":1,"method":"  "}`, `{"jsonrpc":"2.0","error":{"code":-32700,"message":"bad"}}`},
		// KeepEmptyPayload: "" and a missing payload reach the handler empty.
		{`{"jsonrpc":"2.0","id":2,"method":"plugin.invoke","params":{"action":"a","payload":""}}`, `{"jsonrpc":"2.0","id":2,"result":{"action":"a","payload":""}}`},
		{`{"jsonrpc":"2.0","id":3,"method":"plugin.invoke","params":{"action":"a"}}`, `{"jsonrpc":"2.0","id":3,"result":{"action":"a","payload":""}}`},
		{`{"jsonrpc":"2.0","id":null,"method":"plugin.invoke","params":{"action":"a","payload":"{}"}}`, `{"jsonrpc":"2.0","id":null,"result":{"action":"a","payload":"{}"}}`},
	} {
		if got := p.call(c.in); got != c.want {
			t.Errorf("%s\n got %s\nwant %s", c.in, got, c.want)
		}
	}
}
