package main

import "fmt"

// plugin describes how to exercise one official plugin.
type plugin struct {
	store     [2]string // host-backed action answered by the fake host: action, payload
	local     [2]string // deterministic local action (optional)
	heldBusy  int       // number of held invokes that fill the busy gate (0: skip)
	tasks     bool
	taskStart [2]string         // task action that waits on a held host call
	busyCalls int               // held host requests the busy batch produces (default 1; plugins without a store lock: one per held invoke)
	extra     func() []scenario // plugin-specific scenarios appended after the generic ones
}

var plugins = map[string]plugin{
	"nats-client":         {store: [2]string{"listProfiles", "{}"}, heldBusy: 16},
	"kafka-inspector":     {store: [2]string{"listProfiles", "{}"}, heldBusy: 16},
	"zookeeper-manager":   {store: [2]string{"listProfiles", "{}"}, heldBusy: 8},
	"mock-server":         {store: [2]string{"listProfiles", "{}"}, heldBusy: 16, local: [2]string{"status", "{}"}},
	"consul-manager":      {store: [2]string{"listProfiles", "{}"}, heldBusy: 1},
	"devtools":            {local: [2]string{"networkKillProcess", `{"pid":0}`}},
	"pprof-viewer":        {store: [2]string{"listProfiles", "{}"}, heldBusy: 4, tasks: true, taskStart: [2]string{"listProfiles", "{}"}},
	"grpc-debug":          {store: [2]string{"listProfiles", "{}"}, heldBusy: 8, tasks: true, taskStart: [2]string{"listProfiles", "{}"}},
	"docker-manager":      {store: [2]string{"listProfiles", "{}"}, heldBusy: 6, tasks: true, taskStart: [2]string{"listProfiles", "{}"}},
	"http-load-tester":    {store: [2]string{"listProfiles", "{}"}, tasks: true, taskStart: [2]string{"probe", "{}"}},
	"certificate-manager": {store: [2]string{"listAssets", "{}"}, heldBusy: 1, tasks: true, taskStart: [2]string{"listAssets", "{}"}},
	"hbuilder-simulator":  {local: [2]string{"readLog", "{}"}},
	"todo":                {store: [2]string{"state", "{}"}, heldBusy: 1},
	"knowledge-base":      {store: [2]string{"libraries", "{}"}, heldBusy: 1, tasks: true, taskStart: [2]string{"libraries", "{}"}},
	"excalidraw":          {store: [2]string{"docs.list", "{}"}, heldBusy: 1},
}

func req(id any, method string, params string) string {
	idText := fmt.Sprint(id)
	if s, ok := id.(string); ok {
		idText = s
	}
	if params == "" {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"method":%q}`, idText, method)
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"method":%q,"params":%s}`, idText, method, params)
}

func invoke(id any, action, payload string) string {
	return req(id, "plugin.invoke", fmt.Sprintf(`{"action":%q,"payload":%q}`, action, payload))
}

func s(line string) step                 { return step{send: line} }
func barrier(line string) step           { return step{send: line, sync: true} }
func sh(line string, mode hostMode) step { return step{send: line, host: mode} }

const (
	initPlain = `{"runtimeProtocolVersion":1}`
	initTasks = `{"runtimeProtocolVersion":1,"features":{"tasks":1}}`
)

func (p plugin) scenarios() []scenario {
	var out []scenario
	basic := []step{
		s(req(1, "plugin.ping", "")),
		s(invoke(2, "nope", "{}")),
		barrier(`{bad json`),
		barrier(`[]`),
		barrier(`{"jsonrpc":"1.0","id":3,"method":"plugin.ping"}`),
		s(req(4, "plugin.initialize", initPlain)),
		s(req(5, "plugin.ping", "")),
		s(req(6, "plugin.nope", "")),
		s(invoke(7, "nope", "{}")),
		s(req(8, "plugin.invoke", `{"action":"nope","payload":5}`)),
		s(req(9, "plugin.invoke", "")),
		s(req(10, "plugin.invoke", `{"payload":"{}"}`)),
		s(req(11, "plugin.task.status", `{"taskId":"t1"}`)),
		s(req(12, "plugin.task.start", `{"taskId":"t1","action":"nope","payload":"{}"}`)),
	}
	if a := p.store[0]; a != "" {
		basic = append(basic,
			s(req(13, "plugin.invoke", fmt.Sprintf(`{"action":%q,"payload":5}`, a))),
			s(invoke(14, a, "[1]")),
			s(invoke(15, a, "{bad")),
			s(invoke(16, a, "")),
			s(req(17, "plugin.invoke", fmt.Sprintf(`{"action":%q}`, a))),
			s(req(18, "plugin.invoke", fmt.Sprintf(`{"action":%q,"payload":{}}`, a))),
			s(invoke(19, a, p.store[1])),
			s(invoke(20, a, `{"unexpected":true}`)),
		)
	}
	if a := p.local[0]; a != "" {
		basic = append(basic,
			s(req(21, "plugin.invoke", fmt.Sprintf(`{"action":%q,"payload":5}`, a))),
			s(invoke(22, a, "[1]")),
			s(invoke(23, a, "{bad")),
			s(invoke(24, a, p.local[1])),
		)
	}
	basic = append(basic,
		s(req(98, "plugin.shutdown", "")),
		step{send: req(99, "plugin.ping", ""), maybe: true},
		step{eof: true},
	)
	out = append(out, scenario{"basic", basic})

	// Framing probes odd ids and frames; replies may be id-less or absent, so
	// every step ends on the hidden ping barrier.
	out = append(out, scenario{"framing", []step{
		barrier(req(`"s-1"`, "plugin.initialize", initPlain)),
		barrier(req("\"<&>\u2028x\"", "plugin.ping", "")),
		barrier(req(-7, "plugin.ping", "")),
		barrier(req(1.5, "plugin.ping", "")),
		barrier(req("1e3", "plugin.ping", "")),
		barrier(req("0", "plugin.ping", "")),
		barrier(req("null", "plugin.ping", "")),
		barrier(req("true", "plugin.ping", "")),
		barrier(req("{}", "plugin.ping", "")),
		barrier(req(`"`+longID()+`"`, "plugin.ping", "")),
		barrier(`{"jsonrpc":"2.0","method":"plugin.ping"}`),
		barrier(`{"jsonrpc":"2.0","id":2,"method":"plugin.ping","id":3}`),
		barrier(`{"jsonrpc":"2.0","id":4,"method":"plugin.ping","params":"\udc00"}`),
		barrier(`{"jsonrpc":"2.0","id":5,"method":"plugin.ping"} {}`),
		barrier(`   `),
		barrier(``),
		barrier(`{"jsonrpc":"2.0","id":6,"method":5}`),
		barrier(`{"jsonrpc":"2.0","id":"unknown-host-id","result":{}}`),
		barrier(`{"jsonrpc":"2.0","id":7,"method":"plugin.ping","params":"@@PAD1048600@@"}`),
		barrier(req(8, "plugin.ping", "")),
		s(req(9, "plugin.shutdown", "")),
		step{eof: true},
	}})

	out = append(out, scenario{"host-errors", func() []step {
		st := []step{s(req(1, "plugin.initialize", initPlain))}
		if a := p.store[0]; a != "" {
			st = append(st, sh(invoke(2, a, p.store[1]), hostDeny), sh(invoke(3, a, p.store[1]), hostFail))
		}
		return append(st, s(req(4, "plugin.shutdown", "")), step{eof: true})
	}()})

	if p.heldBusy > 0 && p.store[0] != "" {
		// The busy gate is filled by one batch: heldBusy invokes whose host
		// calls are held, then invoke 9 which must be rejected as busy.
		busyCalls := p.busyCalls
		if busyCalls == 0 {
			busyCalls = 1
		}
		var batch []string
		for i := 0; i < p.heldBusy; i++ {
			batch = append(batch, invoke(10+i, p.store[0], p.store[1]))
		}
		batch = append(batch, invoke(9, p.store[0], p.store[1]))
		out = append(out, scenario{"busy", []step{
			s(req(1, "plugin.initialize", initPlain)),
			{batch: batch, host: hostHold, holdCalls: busyCalls},
			{send: req(2, "plugin.shutdown", ""), host: hostHold, drain: true, fuzzy: true},
			{send: req(3, "plugin.ping", ""), maybe: true},
			{eof: true},
		}})

		out = append(out, scenario{"eof-drain", []step{
			s(req(1, "plugin.initialize", initPlain)),
			{send: invoke(2, p.store[0], p.store[1]), host: hostHold, holdCalls: 1},
			{eof: true, host: hostHold, fuzzy: true},
		}})
	}

	if p.tasks {
		a := p.taskStart
		start := func(id any, task string, mode hostMode) step {
			return step{send: req(id, "plugin.task.start", fmt.Sprintf(`{"taskId":%q,"action":%q,"payload":%q}`, task, a[0], a[1])), host: mode, task: task}
		}
		out = append(out, scenario{"tasks", []step{
			s(req(1, "plugin.initialize", initTasks)),
			s(req(2, "plugin.task.status", `{"taskId":"missing"}`)),
			s(req(3, "plugin.task.status", `{}`)),
			s(req(4, "plugin.task.cancel", `{"taskId":"bad id!"}`)),
			s(req(5, "plugin.task.start", `{"taskId":"bad id!","action":"x","payload":"{}"}`)),
			{send: req(6, "plugin.task.start", `{"taskId":"t-nope","action":"nope","payload":"{}"}`), task: "t-nope"},
			{send: req(7, "plugin.task.start", fmt.Sprintf(`{"taskId":"t-bad","action":%q,"payload":"{bad"}`, a[0])), task: "t-bad"},
			start(8, "t-ok", hostAnswer),
			s(req(9, "plugin.task.status", `{"taskId":"t-ok"}`)),
			start(10, "t-held", hostHold),
			s(req(11, "plugin.task.status", `{"taskId":"t-held"}`)),
			sh(req(12, "plugin.task.start", fmt.Sprintf(`{"taskId":"t-held","action":%q,"payload":%q}`, a[0], a[1])), hostHold),
			{send: req(13, "plugin.task.cancel", `{"taskId":"t-held"}`), host: hostHold, task: "t-held", settle: true},
			s(req(14, "plugin.task.status", `{"taskId":"t-held"}`)),
			s(req(15, "plugin.task.cancel", `{"taskId":"t-held"}`)),
			s(req(16, "plugin.invoke", fmt.Sprintf(`{"action":%q,"payload":%q}`, a[0], a[1]))),
			start(17, "t-shut", hostHold),
			{send: req(18, "plugin.shutdown", ""), host: hostHold},
			{eof: true},
		}})
	}
	if p.extra != nil {
		out = append(out, p.extra()...)
	}
	return out
}

func longID() string {
	b := make([]byte, 300)
	for i := range b {
		b[i] = 'a'
	}
	return string(b)
}
