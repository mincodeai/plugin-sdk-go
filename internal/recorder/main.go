// Command recorder drives a built official plugin binary over stdio with a
// scripted set of scenarios and writes golden transcripts
// (testdata/golden/<plugin>/<scenario>.jsonl) for the SDK replay tests.
//
//	go run ./internal/recorder -bin /tmp/bins -out testdata/golden [-j 4] [plugin...]
//
// Recording is deterministic: no step ends on a silence window. Each step
// waits for an explicit completion condition before the next input is sent:
//
//   - a request with a plain id (string or integer) waits for the reply with
//     that id; replies are matched by decoded id, so HTML-escaped ids match too;
//   - a step that may legitimately stay silent (malformed frames, odd ids,
//     notifications, blank lines) is followed by a hidden plugin.ping with a
//     recorder-private id ("rec-sync-N"). The read loop answers it after it
//     has handled the step's line, so its reply is a barrier; the probe and its
//     reply are not written to the transcript;
//   - a held invoke waits for its expected number of held host requests;
//   - a task start that reports "running" waits until the task is terminal
//     (hidden plugin.task.status probes) or, when its host calls are held,
//     until the held host request arrives;
//   - plugin.shutdown waits for its reply and then (bounded) for the process
//     to exit, so later inputs never race the exit;
//   - # eof waits for stdout to close.
//
// Lines are written in a canonical order: each input line is followed by the
// output lines it caused (host replies right after the host request they
// answer). A batch step sends several requests back to back (the busy gate);
// its inputs come first and its outputs follow as one sorted "<~" block that
// replay compares as a multiset. Drained shutdown replies are a sorted "<~"
// block followed by the shutdown reply. Stderr lines are sorted.
//
// Reverse host requests are answered by a fake host (storage/secrets) unless
// the current step holds them, which keeps invokes in flight deterministically.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mincodeai/plugin-sdk-go/rpctest"
)

func main() {
	bin := flag.String("bin", "", "directory holding one binary per plugin id")
	out := flag.String("out", "testdata/golden", "golden output directory")
	jobs := flag.Int("j", 1, "plugins recorded in parallel")
	flag.Parse()
	names := flag.Args()
	if len(names) == 0 {
		for name := range plugins {
			names = append(names, name)
		}
		sort.Strings(names)
	}
	var (
		mu     sync.Mutex
		failed bool
		wg     sync.WaitGroup
	)
	sem := make(chan struct{}, max(*jobs, 1))
	report := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(os.Stderr, format, args...)
		failed = true
	}
	for _, name := range names {
		p, ok := plugins[name]
		if !ok {
			report("unknown plugin %s\n", name)
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			dir := filepath.Join(*out, name)
			_ = os.RemoveAll(dir)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				panic(err)
			}
			for _, sc := range p.scenarios() {
				tr, errs := record(filepath.Join(*bin, name), sc)
				for _, e := range errs {
					report("%s/%s: %s\n", name, sc.name, e)
				}
				if err := os.WriteFile(filepath.Join(dir, sc.name+".jsonl"), tr.Format(), 0o644); err != nil {
					panic(err)
				}
				mu.Lock()
				fmt.Printf("%s/%s: %d lines\n", name, sc.name, len(tr))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if failed {
		os.Exit(1)
	}
}

var fuzzyMessage = regexp.MustCompile(`"error":\{"code":(-?\d+),"message":"(?:[^"\\]|\\.)*"`)

type hostMode int

const (
	hostAnswer hostMode = iota
	hostHold
	hostDeny
	hostFail
)

type step struct {
	send      string
	batch     []string // requests sent back to back; outputs form one unordered block
	eof       bool
	host      hostMode
	holdCalls int  // held host requests the step waits for (batch: in addition to its replies)
	sync      bool // the line may get no (or an id-less) reply: end the step on a hidden ping barrier
	maybe     bool // after shutdown: a reply may or may not come (bounded wait)
	drain     bool // shutdown with requests in flight: outputs unordered, own reply last
	fuzzy     bool // error messages of drained in-flight replies race (ctx vs host close): store {{*}}
	task      string
	settle    bool // task cancel: wait (bounded) until the task leaves "running"
}

// taskSettle is the pause recorded before each request of the tasks scenario.
const taskSettle = "100ms"

const (
	stepTimeout   = 20 * time.Second        // a required reply that does not arrive is an error
	silentWait    = 2 * time.Second         // bounded wait for a reply that may never come
	exitWait      = 3 * time.Second         // bounded wait for the process to exit after shutdown
	settleWait    = 1500 * time.Millisecond // bounded wait for a cancelled task to stop
	extraLineWait = 250 * time.Millisecond  // after a batch: detect lines beyond the expectation
)

type scenario struct {
	name  string
	steps []step
}

type fakeHost struct {
	storage map[string]json.RawMessage
}

func (h *fakeHost) answer(mode hostMode, id json.RawMessage, method string, params json.RawMessage) string {
	reply := func(result string) string {
		return `{"jsonrpc":"2.0","id":` + string(id) + `,"result":` + result + `}`
	}
	fail := func(code int, msg string) string {
		b, _ := json.Marshal(msg)
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"error":{"code":%d,"message":%s}}`, id, code, b)
	}
	switch mode {
	case hostDeny:
		if strings.HasPrefix(method, "host.secrets.") {
			return fail(-32001, "secrets capability not granted")
		}
		return fail(-32001, "storage capability not granted")
	case hostFail:
		return fail(-32002, "disk full")
	}
	var p struct {
		Key   string          `json:"key"`
		Value json.RawMessage `json:"value"`
	}
	_ = json.Unmarshal(params, &p)
	switch method {
	case "host.storage.get":
		if v, ok := h.storage[p.Key]; ok {
			return reply(string(v))
		}
		return reply("null")
	case "host.storage.set":
		h.storage[p.Key] = p.Value
		return reply("true")
	case "host.storage.delete", "host.secrets.set", "host.secrets.delete":
		delete(h.storage, p.Key)
		return reply("true")
	case "host.storage.list":
		keys := []string{}
		for k := range h.storage {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b, _ := json.Marshal(keys)
		return reply(string(b))
	case "host.secrets.list":
		return reply("[]")
	case "host.secrets.get", "host.storage.file.read", "host.storage.file.stat":
		return reply("null")
	case "host.storage.file.list":
		return reply("[]")
	}
	return fail(-32601, "unknown host method")
}

// idKey canonicalises a JSON-RPC id for matching: "s:<string>" or "n:<integer>";
// "" for anything else (null, fractions, objects, absent).
func idKey(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return ""
	}
	if raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return ""
		}
		return "s:" + s
	}
	if plainInt.Match(raw) {
		return "n:" + string(raw)
	}
	return ""
}

var plainInt = regexp.MustCompile(`^-?(0|[1-9]\d*)$`)

type message struct {
	ID     json.RawMessage `json:"id"`
	Method *string         `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
}

func parse(s string) (message, bool) {
	var m message
	return m, json.Unmarshal([]byte(s), &m) == nil
}

// requestKey is the id key of a request line whose reply the step waits for.
func requestKey(line string) string {
	m, ok := parse(line)
	if !ok || m.Method == nil {
		return ""
	}
	return idKey(m.ID)
}

const probePrefix = "rec-sync-"

type recorder struct {
	stdin  io.WriteCloser
	lines  <-chan string
	host   *fakeHost
	closed bool
	exited bool
	probes int
	errs   []string

	// state of the current step
	mode    hostMode
	outs    []rpctest.Line
	replies map[string]string // id key -> reply line
	held    int
	probed  map[string]string // probe id -> reply line
}

func (r *recorder) fail(format string, args ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

func (r *recorder) write(line string) {
	if r.closed {
		return
	}
	_, _ = io.WriteString(r.stdin, line+"\n")
}

func (r *recorder) begin(mode hostMode) {
	r.mode = mode
	r.outs = nil
	r.replies = map[string]string{}
	r.held = 0
	r.probed = map[string]string{}
}

// handle files one stdout line into the current step.
func (r *recorder) handle(s string) {
	m, ok := parse(s)
	key := ""
	if ok {
		key = idKey(m.ID)
	}
	if ok && m.Method == nil && strings.HasPrefix(key, "s:"+probePrefix) {
		r.probed[key[2:]] = s
		return
	}
	r.outs = append(r.outs, rpctest.Line{Kind: rpctest.KindExpect, Text: s})
	if ok && m.Method != nil && strings.HasPrefix(*m.Method, "host.") && key != "" {
		if r.mode == hostHold {
			r.held++
			return
		}
		reply := r.host.answer(r.mode, m.ID, *m.Method, m.Params)
		r.outs = append(r.outs, rpctest.Line{Kind: rpctest.KindSend, Text: reply})
		r.write(reply)
		return
	}
	if key != "" {
		r.replies[key] = s
	}
}

// pump handles stdout lines until done reports true (true), the timeout
// passes or stdout closes (false unless done).
func (r *recorder) pump(timeout time.Duration, done func() bool) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for !done() {
		if r.exited {
			return false
		}
		select {
		case s, ok := <-r.lines:
			if !ok {
				r.exited = true
				return done()
			}
			r.handle(s)
		case <-timer.C:
			return false
		}
	}
	return true
}

func never() bool { return false }

func (r *recorder) replied(key string) func() bool {
	return func() bool { _, ok := r.replies[key]; return ok }
}

// probe sends a hidden request and returns its reply ("" if none arrived).
func (r *recorder) probe(method, params string) string {
	if r.closed || r.exited {
		return ""
	}
	r.probes++
	id := fmt.Sprintf("%s%d", probePrefix, r.probes)
	r.write(req(`"`+id+`"`, method, params))
	r.pump(stepTimeout, func() bool { _, ok := r.probed[id]; return ok })
	return r.probed[id]
}

// taskState reads result.state of a task reply line.
func taskState(line string) string {
	var m struct {
		Result struct {
			State string `json:"state"`
		} `json:"result"`
	}
	_ = json.Unmarshal([]byte(line), &m)
	return m.Result.State
}

func (r *recorder) status(task string) string {
	return taskState(r.probe("plugin.task.status", fmt.Sprintf(`{"taskId":%q}`, task)))
}

// waitTask blocks until a task reported "running" is terminal, or (held host
// calls) until it issued its held host request.
func (r *recorder) waitTask(st step, reply string) {
	if taskState(reply) != "running" {
		return
	}
	limit := stepTimeout
	if st.settle {
		limit = settleWait
	}
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) && !r.exited {
		if st.host == hostHold && !st.settle && r.held > 0 {
			return
		}
		if s := r.status(st.task); s != "running" && s != "" {
			return
		}
		r.pump(20*time.Millisecond, func() bool { return st.host == hostHold && !st.settle && r.held > 0 })
	}
	if !st.settle {
		r.fail("task %s still running after %v", st.task, limit)
	}
}

func isShutdown(line string) bool {
	m, ok := parse(line)
	return ok && m.Method != nil && *m.Method == "plugin.shutdown"
}

// flush appends the step's outputs to tr in canonical order.
func (r *recorder) flush(tr rpctest.Transcript, st step, own string) rpctest.Transcript {
	outs := r.outs
	if st.fuzzy {
		for i := range outs {
			if outs[i].Kind == rpctest.KindExpect {
				outs[i].Text = fuzzyMessage.ReplaceAllString(outs[i].Text, `"error":{"code":$1,"message":"{{*}}"`)
			}
		}
	}
	unordered := func(ls []rpctest.Line) rpctest.Transcript {
		var texts []string
		for _, l := range ls {
			if l.Kind != rpctest.KindExpect {
				r.fail("host reply inside an unordered block: %s", l.Text)
			}
			texts = append(texts, l.Text)
		}
		sort.Strings(texts)
		if len(texts) == 1 {
			return append(tr, rpctest.Line{Kind: rpctest.KindExpect, Text: texts[0]})
		}
		for _, t := range texts {
			tr = append(tr, rpctest.Line{Kind: rpctest.KindUnordered, Text: t})
		}
		return tr
	}
	switch {
	case len(st.batch) > 0, st.eof && st.fuzzy:
		return unordered(outs)
	case st.drain:
		n := len(outs)
		if n > 0 && own != "" && idKey(mustID(outs[n-1].Text)) == own {
			tr = unordered(outs[:n-1])
			return append(tr, outs[n-1])
		}
		if n > 1 {
			r.fail("shutdown reply is not the last drained line")
		}
		return unordered(outs)
	}
	return append(tr, outs...)
}

func mustID(line string) json.RawMessage {
	m, _ := parse(line)
	return m.ID
}

func record(bin string, sc scenario) (rpctest.Transcript, []string) {
	cmd := exec.Command(bin)
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		return nil, []string{err.Error()}
	}
	lines := make(chan string, 1024)
	go func() {
		r := bufio.NewScanner(stdout)
		r.Buffer(make([]byte, 64*1024), 16<<20)
		for r.Scan() {
			lines <- r.Text()
		}
		close(lines)
	}()
	var errLines []string
	errDone := make(chan struct{})
	go func() {
		r := bufio.NewScanner(stderr)
		for r.Scan() {
			errLines = append(errLines, r.Text())
		}
		close(errDone)
	}()
	r := &recorder{stdin: stdin, lines: lines, host: &fakeHost{storage: map[string]json.RawMessage{}}}
	var tr rpctest.Transcript
	closeStdin := func() {
		_ = stdin.Close()
		r.closed = true
		tr = append(tr, rpctest.Line{Kind: rpctest.KindEOF})
	}
	for n, st := range sc.steps {
		if r.exited {
			break
		}
		r.begin(st.host)
		own := ""
		switch {
		case st.eof:
			closeStdin()
			if !r.pump(stepTimeout, never) && !r.exited {
				r.fail("plugin kept stdout open after stdin closed")
			}
		case len(st.batch) > 0:
			for _, line := range st.batch {
				tr = append(tr, rpctest.Line{Kind: rpctest.KindSend, Text: line})
				r.write(line)
			}
			// Held requests never reply; only the last request (the busy
			// rejection) does, next to holdCalls held host requests.
			want := []string{requestKey(st.batch[len(st.batch)-1])}
			done := func() bool {
				for _, k := range want {
					if _, ok := r.replies[k]; !ok {
						return false
					}
				}
				return r.held >= st.holdCalls
			}
			if !r.pump(stepTimeout, done) {
				r.fail("batch: got %d held host requests and replies %v, want %d and %v", r.held, keysOf(r.replies), st.holdCalls, want)
			}
			before := len(r.outs)
			r.pump(extraLineWait, never)
			if len(r.outs) > before {
				r.fail("batch: %d unexpected extra lines, e.g. %s", len(r.outs)-before, r.outs[before].Text)
			}
		default:
			if sc.name == "tasks" && n > 0 {
				// Task work finishes asynchronously; replay must let it settle
				// before the next request observes its state.
				tr = append(tr, rpctest.Line{Kind: rpctest.KindWait, Text: taskSettle})
			}
			tr = append(tr, rpctest.Line{Kind: rpctest.KindSend, Text: st.send})
			r.write(rpctest.ExpandPad(st.send))
			own = requestKey(st.send)
			switch {
			case st.sync || own == "":
				r.probe("plugin.ping", "")
				responded := false
				for _, l := range r.outs {
					if m, ok := parse(l.Text); ok && m.Method == nil {
						responded = true
					}
				}
				if own != "" && !responded {
					r.pump(silentWait, r.replied(own))
				}
			case st.maybe:
				r.pump(silentWait, r.replied(own))
			case st.host == hostHold && st.holdCalls > 0:
				if !r.pump(stepTimeout, func() bool { return r.held >= st.holdCalls }) {
					r.fail("%s: got %d held host requests, want %d", own, r.held, st.holdCalls)
				}
			default:
				if !r.pump(stepTimeout, r.replied(own)) {
					r.fail("no reply to %s", own)
					break
				}
				if st.task != "" {
					r.waitTask(st, r.replies[own])
				}
			}
			if isShutdown(st.send) {
				r.pump(exitWait, never)
			}
		}
		tr = r.flush(tr, st, own)
	}
	if !r.closed {
		r.begin(hostAnswer)
		closeStdin()
		r.pump(stepTimeout, never)
		tr = r.flush(tr, step{}, "")
	}
	<-errDone
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()
	select {
	case <-waitErr:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		r.fail("plugin did not exit")
	}
	sort.Strings(errLines)
	for _, s := range errLines {
		tr = append(tr, rpctest.Line{Kind: rpctest.KindStderr, Text: s})
	}
	return tr, r.errs
}

func keysOf(m map[string]string) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
