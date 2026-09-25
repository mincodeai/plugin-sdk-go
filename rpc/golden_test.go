package rpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/jsonx"
	"github.com/mincodeai/plugin-sdk-go/rpc"
	"github.com/mincodeai/plugin-sdk-go/rpctest"
)

// The golden transcripts in testdata/golden were recorded from the original
// plugin binaries (internal/recorder). Each one is replayed here against
// rpc.Serve configured with the plugin's preset and a stub invoke that makes
// the same reverse host requests; the output must match byte for byte.

type hostCall struct {
	method string
	params json.RawMessage
}

func get(key string) hostCall {
	return hostCall{"host.storage.get", json.RawMessage(fmt.Sprintf(`{"key":%q}`, key))}
}

var secretsList = hostCall{"host.secrets.list", json.RawMessage(`{}`)}

type stubAction struct {
	calls  []hostCall
	result string
	err    *rpc.Error
	// ifNull runs after calls when the last result was null (first-run
	// migration, e.g. mock-server seeding its profiles document).
	ifNull []hostCall
}

// stub is a fake plugin: fixed host calls per action, plugin-owned payload
// validation and serialised storage access (like the plugins' store mutex).
type stub struct {
	mu           sync.Mutex
	actions      map[string]stubAction
	checkPayload func(json.RawMessage) error
	ignoreCancel bool
}

func (s *stub) known(action string) bool { _, ok := s.actions[action]; return ok }

func (s *stub) handle(ctx context.Context, action string, payload json.RawMessage) (any, error) {
	a := s.actions[action]
	if s.checkPayload != nil {
		if err := s.checkPayload(payload); err != nil {
			return nil, err
		}
	}
	if a.err != nil {
		return nil, a.err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ignoreCancel {
		ctx = context.WithoutCancel(ctx)
	}
	h := host.FromContext(ctx)
	last := json.RawMessage("null")
	for _, c := range a.calls {
		r, err := h.Call(ctx, c.method, c.params)
		if err != nil {
			return nil, err
		}
		last = r
	}
	if string(last) == "null" {
		for _, c := range a.ifNull {
			if _, err := h.Call(ctx, c.method, c.params); err != nil {
				return nil, err
			}
		}
	}
	return json.RawMessage(a.result), nil
}

// strictSpec collects the per-plugin texts of the Go-native family.
type strictSpec struct {
	tag, fatal, busy, invalidParams string
	maxInflight                     int
	drain                           bool
	unknownAction                   func(action string) string
	notObject, badJSON              string
	unknownField                    func(field string) string
	denied, failed                  string
	secrets                         bool
	storeResult                     string
	tasks                           *rpc.TaskOptions
	rejectTasks                     bool
	extra                           map[string]stubAction
}

func quoted(prefix string) func(string) string {
	return func(s string) string { return prefix + fmt.Sprintf("%q", s) }
}

func fixed(msg string) func(string) string { return func(string) string { return msg } }

func strictPreset(id string, sp strictSpec) (rpc.Options, *stub) {
	o := rpc.StrictDefaults(id)
	o.FatalMessage = sp.fatal
	o.MaxInflight = sp.maxInflight
	o.Busy = rpc.NewError(rpc.CodeBusy, sp.busy)
	o.InvalidParams = rpc.NewError(rpc.CodeInvalidParams, sp.invalidParams)
	o.DrainReplies = sp.drain
	o.Tasks = sp.tasks
	o.RejectTaskMethods = sp.rejectTasks
	o.UnknownAction = func(a string) *rpc.Error { return rpc.NewError(rpc.CodeInvalidParams, sp.unknownAction(a)) }
	o.MapError = func(err error) *rpc.Error {
		switch {
		case host.IsCode(err, host.CodeCapabilityMissing):
			return rpc.NewError(rpc.CodeServerError, sp.denied)
		case errors.As(err, new(*host.Error)):
			return rpc.NewError(rpc.CodeServerError, sp.failed)
		}
		return nil
	}
	calls := []hostCall{get("profiles")}
	if sp.secrets {
		calls = append(calls, secretsList)
	}
	st := &stub{actions: map[string]stubAction{"listProfiles": {calls: calls, result: sp.storeResult}}}
	for k, v := range sp.extra {
		st.actions[k] = v
	}
	st.checkPayload = func(p json.RawMessage) error {
		var empty struct{}
		err := jsonx.DecodeObject(p, &empty, jsonx.DecodeOptions{Strict: true, DisallowUnknown: true})
		var de *jsonx.DecodeError
		if !errors.As(err, &de) {
			return err
		}
		msg := sp.badJSON
		switch de.Kind {
		case jsonx.NotObject:
			msg = sp.notObject
		case jsonx.UnknownField:
			msg = sp.unknownField(de.Field)
		}
		return rpc.NewError(rpc.CodeInvalidParams, msg)
	}
	o.KnownAction = st.known
	o.Handler = st.handle
	return o, st
}

func strictTasks(tag string) *rpc.TaskOptions {
	return &rpc.TaskOptions{Cancelled: "[" + tag + "_CANCELLED] Task cancelled."}
}

func preset(t *testing.T, plugin string) rpc.Options {
	if f, ok := extraPresets[plugin]; ok {
		return f(t)
	}
	const (
		natsInvalid  = "[NATS_INPUT] Invalid input or unsupported option."
		kafkaInvalid = "[KAFKA_INPUT] Invalid input or unsupported option."
		zkInvalid    = "[ZK_INPUT] Invalid input or unsupported option."
	)
	lower := func(tag string) (string, string) {
		return "[" + tag + "_STORAGE] host storage is not granted to this plugin", "[" + tag + "_STORAGE] host storage request failed"
	}
	upper := func(tag string) (string, string) {
		return "[" + tag + "_STORAGE] Host storage is not granted to this plugin.", "[" + tag + "_STORAGE] Host storage request failed."
	}
	var o rpc.Options
	switch plugin {
	case "nats-client", "kafka-inspector":
		tag, name, busy, invalid := "NATS", "NATS", "请求过多，请稍后重试 / Too many concurrent requests", natsInvalid
		if plugin == "kafka-inspector" {
			tag, name, busy, invalid = "KAFKA", "Kafka", "[KAFKA_BUSY] 请求过多，请稍后重试 / Too many concurrent requests", kafkaInvalid
		}
		denied, failed := upper(tag)
		o, _ = strictPreset(plugin, strictSpec{
			fatal: name + " plugin transport failed", busy: busy, invalidParams: invalid, maxInflight: 16, drain: true,
			unknownAction: quoted("[" + tag + "_INPUT] unknown action "), notObject: invalid, badJSON: invalid,
			unknownField: quoted("[" + tag + "_INPUT] unknown field "), denied: denied, failed: failed,
			secrets: true, storeResult: `{"max":50,"profiles":[]}`,
		})
	case "zookeeper-manager":
		o, _ = strictPreset(plugin, strictSpec{
			fatal: "ZooKeeper plugin transport failed", busy: "Too many concurrent requests; retry shortly", invalidParams: zkInvalid, maxInflight: 8,
			unknownAction: fixed("[ZK_INPUT] Unknown action."), notObject: zkInvalid, badJSON: zkInvalid,
			unknownField: func(f string) string { return "[ZK_INPUT] Unsupported or null field: " + f + "." },
			denied:       "[ZK_STORAGE] Plugin storage is not granted to this plugin.",
			failed:       "[ZK_STORAGE] Plugin storage is unavailable; profiles need the MinCode desktop app.",
			secrets:      true, storeResult: `{"connected":[],"max":50,"profiles":[]}`,
		})
	case "mock-server":
		denied, failed := lower("MOCK")
		var st *stub
		o, st = strictPreset(plugin, strictSpec{
			fatal: "mock-server plugin transport failed", busy: "too many concurrent requests, retry later", invalidParams: "[MOCK_INPUT] invalid invoke params", maxInflight: 16,
			unknownAction: fixed("[MOCK_INPUT] unknown action"), notObject: "[MOCK_INPUT] payload must be a JSON object",
			badJSON:      "[MOCK_INPUT] payload is not strict JSON (duplicate keys, invalid UTF-8 or unpaired surrogates)",
			unknownField: quoted("[MOCK_INPUT] invalid payload: json: unknown field "), denied: denied, failed: failed,
			extra: map[string]stubAction{"status": {result: `{"captureCapacity":500,"captured":0,"configId":"default","latestSeq":0,"maxServers":4,"running":false,"servers":[]}`}},
		})
		st.actions["listProfiles"] = stubAction{calls: []hostCall{get("profiles")}, ifNull: []hostCall{get("rules.v1"),
			{"host.storage.set", json.RawMessage(`{"key":"profiles","value":{"version":1,"profiles":[]}}`)}},
			result: `{"dropped":0,"max":50,"migrated":false,"profiles":[]}`}
	case "consul-manager":
		denied, failed := lower("CONSUL")
		o, _ = strictPreset(plugin, strictSpec{
			fatal: "Consul plugin transport failed", busy: "已有任务运行，请稍后重试", invalidParams: "[CONSUL_INPUT] invalid invoke envelope", maxInflight: 1,
			unknownAction: fixed("[CONSUL_INPUT] unknown action"), notObject: "[CONSUL_INPUT] payload must be a JSON object", badJSON: "[CONSUL_INPUT] payload must be a JSON object",
			unknownField: quoted("[CONSUL_INPUT] payload has unsupported field "), denied: denied, failed: failed,
			secrets: true, storeResult: `{"dropped":0,"max":50,"profiles":[]}`,
		})
	case "pprof-viewer":
		denied, failed := lower("PPROF")
		o, _ = strictPreset(plugin, strictSpec{
			fatal: "pprof plugin transport failed", busy: "插件繁忙，请稍后重试 / Plugin busy, try again later", invalidParams: "[PPROF_INPUT] Invalid invoke parameters.", maxInflight: 4,
			unknownAction: fixed("[PPROF_INPUT] unsupported action"), notObject: "[PPROF_INPUT] payload must be a JSON object", badJSON: "[PPROF_INPUT] payload must be a JSON object",
			unknownField: quoted("[PPROF_INPUT] invalid payload: unsupported field "), denied: denied, failed: failed,
			secrets: true, storeResult: `{"dropped":0,"max":50,"profiles":[],"secretsUnavailable":false}`,
			tasks: strictTasks("PPROF"), rejectTasks: true,
		})
	case "grpc-debug":
		denied, failed := upper("GRPC")
		o, _ = strictPreset(plugin, strictSpec{
			fatal: "grpc-debug plugin transport failed", busy: "插件繁忙，请稍后重试 / Plugin busy, try again later", invalidParams: "[GRPC_INPUT] Invalid invoke parameters.", maxInflight: 8,
			unknownAction: fixed("[GRPC_INPUT] unknown action"), notObject: "[GRPC_INPUT] payload must be a JSON object", badJSON: "[GRPC_INPUT] Invalid input or unsupported option.",
			unknownField: quoted("[GRPC_INPUT] unknown field "), denied: denied, failed: failed,
			secrets: true, storeResult: `{"max":50,"profiles":[]}`,
			tasks: strictTasks("GRPC"), rejectTasks: true,
		})
	case "docker-manager":
		denied, failed := lower("DOCKER")
		o, _ = strictPreset(plugin, strictSpec{
			fatal: "docker-manager: transport failed", busy: "Too many Docker requests in flight; retry shortly", invalidParams: "[DOCKER_INPUT] invalid invoke parameters", maxInflight: 6,
			unknownAction: fixed("[DOCKER_INPUT] unknown action"), notObject: "[DOCKER_INPUT] payload must be a JSON object", badJSON: "[DOCKER_INPUT] payload must be a strict JSON object",
			unknownField: quoted("[DOCKER_INPUT] unsupported field "), denied: denied, failed: failed,
			secrets: true, storeResult: `{"dropped":0,"max":50,"profiles":[]}`, rejectTasks: true,
			tasks: &rpc.TaskOptions{
				InvalidID: "Invalid task ID", DuplicateID: "Duplicate task ID", LookupInvalidID: "Invalid task ID",
				Prepare: func(action string, _ json.RawMessage) error {
					return rpc.NewError(rpc.CodeInvalidParams, fmt.Sprintf("[DOCKER_INPUT] action %q cannot run as a task", action))
				},
			},
		})
	case "http-load-tester":
		denied, failed := lower("HTTP_LOAD")
		o, _ = strictPreset(plugin, strictSpec{
			fatal: "http-load-tester plugin transport failed", busy: "too many concurrent requests", invalidParams: "[HTTP_LOAD_INPUT] invoke params must be {action, payload}", maxInflight: 8,
			unknownAction: fixed("[HTTP_LOAD_INPUT] unsupported action"), notObject: "[HTTP_LOAD_INPUT] payload must be a JSON object",
			badJSON:      "[HTTP_LOAD_INPUT] payload must be a strict JSON object (valid UTF-8, no duplicate fields)",
			unknownField: quoted("[HTTP_LOAD_INPUT] unsupported field "), denied: denied, failed: failed,
			secrets: true, storeResult: `{"max":50,"profiles":[]}`, rejectTasks: true,
			extra: map[string]stubAction{"probe": {err: rpc.NewError(rpc.CodeInvalidParams, "[HTTP_LOAD_INPUT] url is required")}},
			tasks: &rpc.TaskOptions{
				Prepare: func(string, json.RawMessage) error {
					return errors.New("[HTTP_LOAD_INPUT] only the run action can be started as a task")
				},
			},
		})
	default:
		o = nodePreset(t, plugin)
	}
	return o
}

func nodePreset(t *testing.T, plugin string) rpc.Options {
	o := rpc.NodeDefaults()
	st := &stub{actions: map[string]stubAction{}}
	o.RejectTaskMethods = true
	switch plugin {
	case "devtools":
		o.RequireVersion = true
		o.NonStringMethod = rpc.MethodTypeInvalid
		o.IDs = rpc.IDNonNull
		o.EscapeHTML = true
		o.RejectTaskMethods = false
		st.actions["networkKillProcess"] = stubAction{err: rpc.NewError(rpc.CodeServerError, "PID 无效")}
	case "certificate-manager":
		o.Style = rpc.StyleSorted
		o.Host.SortKeys = true
		o.SkipEmptyLines = true
		o.NonStringMethod = rpc.MethodTypeIgnore
		o.Tasks = &rpc.TaskOptions{SharedGate: true}
		st.actions["listAssets"] = stubAction{calls: []hostCall{get("certificate-manager.assets")}, result: `{"result":[]}`}
	case "hbuilder-simulator":
		o.Style = rpc.StyleSorted
		o.Host.SortKeys = true
		o.SkipBlankLines = true
		st.actions["readLog"] = stubAction{result: `{"h5":{"running":false},"output":""}`}
	case "todo":
		st.actions["state"] = stubAction{calls: []hostCall{get("tasks"), get("lists")}, result: `{"tasks":[],"archived":[],"lists":[]}`}
	case "knowledge-base":
		o.Style = rpc.StyleJS
		o.Tasks = &rpc.TaskOptions{SharedGate: true}
		st.ignoreCancel = true
		st.actions["libraries"] = stubAction{
			calls:  []hostCall{{"host.storage.list", json.RawMessage(`{}`)}, get("libraries")},
			result: `[{"id":"default","title":"我的知识库","description":"","project":"","favorite":true,"created":"1970-01-01T00:00:00.000Z","count":0}]`,
		}
	case "excalidraw":
		o.Style = rpc.StyleJS
		st.actions["docs.list"] = stubAction{calls: []hostCall{get("docs-index-v1")}, result: `[]`}
	default:
		t.Fatalf("no preset for %s", plugin)
	}
	o.KnownAction = st.known
	o.Handler = st.handle
	return o
}

// goldenPlugins are the plugins with recorded transcripts; etcd-manager and
// network-diagnostics are recorded after their in-flight rewrites land.
var goldenPlugins = []string{
	"nats-client", "zookeeper-manager", "kafka-inspector", "mock-server", "consul-manager",
	"devtools", "pprof-viewer", "docker-manager", "http-load-tester", "grpc-debug",
	"certificate-manager", "hbuilder-simulator", "todo", "knowledge-base", "excalidraw",
}

// extraPresets lets later golden batches register their plugin preset from
// their own test file (func init() { extraPresets["x"] = ... }).
var extraPresets = map[string]func(*testing.T) rpc.Options{}

// allGoldenPlugins is goldenPlugins plus the registered extra presets.
func allGoldenPlugins() []string {
	all := append([]string(nil), goldenPlugins...)
	var extra []string
	for name := range extraPresets {
		extra = append(extra, name)
	}
	sort.Strings(extra)
	return append(all, extra...)
}

func TestGoldenTranscripts(t *testing.T) {
	dirs, err := filepath.Glob(filepath.Join("..", "testdata", "golden", "*"))
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, d := range dirs {
		found = append(found, filepath.Base(d))
	}
	want := allGoldenPlugins()
	sort.Strings(want)
	sort.Strings(found)
	if strings.Join(found, ",") != strings.Join(want, ",") {
		t.Fatalf("golden plugins = %v, want %v", found, want)
	}
	for _, plugin := range allGoldenPlugins() {
		files, _ := filepath.Glob(filepath.Join("..", "testdata", "golden", plugin, "*.jsonl"))
		if len(files) < 3 {
			t.Errorf("%s: only %d transcripts", plugin, len(files))
		}
		for _, f := range files {
			name := plugin + "/" + strings.TrimSuffix(filepath.Base(f), ".jsonl")
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				tr, err := rpctest.ReadFile(f)
				if err != nil {
					t.Fatal(err)
				}
				rpctest.Replay(t, tr, func(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer) error {
					return rpc.Serve(ctx, stdin, stdout, stderr, preset(t, plugin))
				})
			})
		}
	}
}

// TestGoldenFilesParse guards the transcript format itself.
func TestGoldenFilesParse(t *testing.T) {
	files, _ := filepath.Glob(filepath.Join("..", "testdata", "golden", "*", "*.jsonl"))
	re := regexp.MustCompile(`^(> |< |<~ |! |# )`)
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
			if !re.MatchString(line) && line != "> " && line != ">    " {
				t.Errorf("%s:%d: unexpected line %q", f, i+1, line)
			}
		}
		if _, err := rpctest.Parse(data); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}
