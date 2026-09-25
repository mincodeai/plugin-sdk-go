package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/jsonx"
	"github.com/mincodeai/plugin-sdk-go/jsvalue"
)

var defaultTaskID = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

type task struct {
	ID              string          `json:"taskId"`
	State           string          `json:"state"`
	CancelRequested bool            `json:"cancelRequested"`
	Progress        json.RawMessage `json:"progress,omitempty"`
	Result          json.RawMessage `json:"result,omitempty"`
	Error           string          `json:"error,omitempty"`
	cancel          context.CancelFunc
	finished        time.Time
}

type taskTable struct {
	o       TaskOptions
	tasks   map[string]*task
	running int
}

func newTaskTable(o *TaskOptions) *taskTable {
	t := &taskTable{o: *o, tasks: map[string]*task{}}
	d := &t.o
	if d.IDPattern == nil {
		d.IDPattern = defaultTaskID
	}
	if d.MaxRetained <= 0 {
		d.MaxRetained = 64
	}
	if d.MaxRunning <= 0 {
		d.MaxRunning = 2
	}
	if d.Deadline <= 0 {
		d.Deadline = 300 * time.Second
	}
	if d.Retention <= 0 {
		d.Retention = 5 * time.Minute
	}
	if d.MaxResultBytes <= 0 {
		d.MaxResultBytes = 256 * 1024
	}
	if d.MaxProgressBytes <= 0 {
		d.MaxProgressBytes = 4096
	}
	if d.MaxErrorBytes <= 0 {
		d.MaxErrorBytes = 1000
	}
	if d.NotFound == "" {
		d.NotFound = "Task not found"
	}
	if d.InvalidID == "" {
		d.InvalidID = "Invalid or duplicate task ID"
	}
	if d.DuplicateID == "" {
		d.DuplicateID = d.InvalidID
	}
	if d.LimitReached == nil {
		d.LimitReached = NewError(CodeServerError, "Task count limit reached")
	}
	if d.Busy == nil {
		d.Busy = NewError(CodeBusy, "Too many running tasks; try again later")
	}
	if d.Cancelled == "" {
		d.Cancelled = "Task cancelled"
	}
	if d.ResultTooLarge == "" {
		d.ResultTooLarge = "Task result exceeds limit"
	}
	return t
}

func (t *taskTable) pruneLocked() {
	for id, x := range t.tasks {
		if !x.finished.IsZero() && time.Since(x.finished) > t.o.Retention {
			delete(t.tasks, id)
		}
	}
}

func (t *taskTable) cancelAllLocked() {
	for _, x := range t.tasks {
		if x.State == "running" {
			x.CancelRequested = true
		}
	}
}

func (t *taskTable) snapshot(x *task) json.RawMessage {
	if t.o.SnapshotJS {
		var b bytes.Buffer
		b.WriteString(`{"taskId":`)
		jsvalue.WriteString(&b, x.ID)
		b.WriteString(`,"state":`)
		jsvalue.WriteString(&b, x.State)
		b.WriteString(`,"cancelRequested":`)
		jsvalue.Write(&b, x.CancelRequested)
		for _, m := range []struct {
			key string
			raw json.RawMessage
		}{{"progress", x.Progress}, {"result", x.Result}} {
			if len(m.raw) > 0 {
				b.WriteString(`,"` + m.key + `":`)
				b.Write(m.raw)
			}
		}
		if x.Error != "" {
			b.WriteString(`,"error":`)
			jsvalue.WriteString(&b, x.Error)
		}
		b.WriteByte('}')
		return b.Bytes()
	}
	raw, _ := jsonx.MarshalStyle(x, t.o.SnapshotEscapeHTML)
	return raw
}

func (s *server) taskLookupLocked(id, params json.RawMessage, cancel bool) {
	t := s.tasks
	t.pruneLocked()
	var p struct {
		TaskID string `json:"taskId"`
	}
	if json.Unmarshal(params, &p) != nil && t.o.LookupBadParams != "" {
		s.replyLocked(id, nil, NewError(CodeInvalidParams, t.o.LookupBadParams))
		return
	}
	if t.o.LookupInvalidID != "" && !t.o.IDPattern.MatchString(p.TaskID) {
		s.replyLocked(id, nil, NewError(CodeInvalidParams, t.o.LookupInvalidID))
		return
	}
	x := t.tasks[p.TaskID]
	if x == nil {
		s.replyLocked(id, nil, NewError(CodeInvalidParams, t.o.NotFound))
		return
	}
	if cancel && x.State == "running" {
		x.CancelRequested = true
		x.cancel()
	}
	s.replyLocked(id, t.snapshot(x), nil)
}

func (s *server) taskStartLocked(id, params json.RawMessage) {
	t := s.tasks
	t.pruneLocked()
	var taskID string
	var p invokeParams
	if t.o.SharedGate {
		var perr *Error
		if p, perr = s.validateInvokeLocked(params); perr != nil {
			s.replyLocked(id, nil, perr)
			return
		}
		var q struct {
			TaskID any `json:"taskId"`
		}
		_ = json.Unmarshal(params, &q)
		str, ok := q.TaskID.(string)
		if !ok || !t.o.IDPattern.MatchString(str) || t.tasks[str] != nil {
			s.replyLocked(id, nil, NewError(CodeInvalidParams, t.o.InvalidID))
			return
		}
		taskID = str
	} else if t.o.CheckPayload {
		var q struct {
			TaskID  string          `json:"taskId"`
			Action  string          `json:"action"`
			Payload json.RawMessage `json:"payload"`
		}
		if json.Unmarshal(params, &q) != nil || !t.o.IDPattern.MatchString(q.TaskID) {
			s.replyLocked(id, nil, NewError(CodeInvalidParams, t.o.InvalidID))
			return
		}
		if t.tasks[q.TaskID] != nil {
			s.replyLocked(id, nil, NewError(CodeInvalidParams, t.o.DuplicateID))
			return
		}
		if s.o.KnownAction != nil && !s.o.KnownAction(q.Action) {
			s.replyLocked(id, nil, s.o.UnknownAction(q.Action))
			return
		}
		payload, perr := s.normalizePayload(q.Payload)
		if perr != nil {
			s.replyLocked(id, nil, perr)
			return
		}
		taskID, p = q.TaskID, invokeParams{action: q.Action, payload: payload}
	} else {
		var q struct {
			TaskID  string `json:"taskId"`
			Action  string `json:"action"`
			Payload string `json:"payload"`
		}
		if json.Unmarshal(params, &q) != nil || !t.o.IDPattern.MatchString(q.TaskID) {
			s.replyLocked(id, nil, NewError(CodeInvalidParams, t.o.InvalidID))
			return
		}
		if t.tasks[q.TaskID] != nil {
			s.replyLocked(id, nil, NewError(CodeInvalidParams, t.o.DuplicateID))
			return
		}
		if q.Payload == "" {
			q.Payload = "{}"
		}
		taskID, p = q.TaskID, invokeParams{action: q.Action, payload: json.RawMessage(q.Payload)}
	}
	if t.o.Admit != nil {
		if e := t.o.Admit(p.action, p.payload); e != nil {
			s.replyLocked(id, nil, e)
			return
		}
	}
	if len(t.tasks) >= t.o.MaxRetained {
		s.replyLocked(id, nil, t.o.LimitReached)
		return
	}
	if !t.o.SharedGate && t.running >= t.o.MaxRunning {
		s.replyLocked(id, nil, t.o.Busy)
		return
	}
	x := &task{ID: taskID, State: "running"}
	var run TaskFunc
	prepare := t.o.Prepare
	if t.o.Start != nil {
		prepare = func(action string, payload json.RawMessage) (err error) {
			run, err = t.o.Start(action, payload)
			return err
		}
	}
	if prepare != nil {
		if err := prepare(p.action, p.payload); err != nil {
			x.State, x.Error, x.finished = "failed", t.clip(s.mapError(err).Message), time.Now()
			x.cancel = func() {}
			t.tasks[taskID] = x
			s.replyLocked(id, t.snapshot(x), nil)
			return
		}
	}
	var ctx context.Context
	var cancel context.CancelFunc
	if t.o.FlagOnlyCancel {
		ctx, cancel = context.WithCancel(s.ctx)
		x.cancel = func() {}
		deadline := time.AfterFunc(t.o.Deadline, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if x.State == "running" {
				x.CancelRequested = true
			}
		})
		stop := cancel
		cancel = func() { deadline.Stop(); stop() }
	} else {
		ctx, cancel = context.WithTimeout(s.ctx, t.o.Deadline)
		x.cancel = cancel
	}
	t.tasks[taskID] = x
	t.running++
	if t.o.SharedGate {
		s.inflight++
	}
	s.replyLocked(id, t.snapshot(x), nil)
	report := func(v any) {
		b, err := json.Marshal(v)
		if err != nil || len(b) > t.o.MaxProgressBytes {
			return
		}
		s.mu.Lock()
		if x.State == "running" {
			x.Progress = b
		}
		s.mu.Unlock()
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer cancel()
		tctx := context.WithValue(ctx, progressKey{}, report)
		var result any
		var err error
		if run != nil {
			result, err = s.callTask(tctx, run)
		} else {
			result, err = s.call(tctx, p.action, p.payload)
		}
		if tr, ok := result.(*TaskResult); ok && err == nil {
			s.mu.Lock()
			defer s.mu.Unlock()
			t.running--
			if t.o.SharedGate {
				s.inflight--
			}
			x.finished = time.Now()
			x.State, x.Result, x.Error = tr.State, tr.Result, tr.Error
			x.CancelRequested = x.CancelRequested || tr.CancelRequested
			return
		}
		var raw json.RawMessage
		if err == nil {
			raw, err = s.marshalResult(result)
			if err == nil && (!json.Valid(raw) || len(raw) > t.o.MaxResultBytes) {
				err = errTaskResult
			}
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		t.running--
		if t.o.SharedGate {
			s.inflight--
		}
		x.finished = time.Now()
		switch {
		case errors.Is(err, errTaskResult):
			x.State, x.Error = "failed", t.o.ResultTooLarge
		case err == nil:
			x.State, x.Result = "succeeded", raw
			if t.o.ClearProgressOnSuccess {
				x.Progress = nil
			}
		case !t.o.FlagOnlyCancel && x.CancelRequested && errors.Is(ctx.Err(), context.Canceled):
			x.State, x.Error = "cancelled", t.o.Cancelled
		default:
			msg := s.mapError(err).Message
			if t.o.Timeout != "" && errors.Is(ctx.Err(), context.DeadlineExceeded) {
				msg = t.o.Timeout
			}
			x.State, x.Error = "failed", t.clip(msg)
		}
	}()
}

var errTaskResult = errors.New("task result too large")

// callTask runs TaskOptions.Start work with the host caller attached,
// converting panics like call.
func (s *server) callTask(ctx context.Context, run TaskFunc) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			result, err = nil, &panicError{r}
		}
	}()
	return run(host.WithCaller(ctx, s.host))
}

func (t *taskTable) clip(msg string) string {
	if t.o.ClipError != nil {
		return t.o.ClipError(msg)
	}
	if len(msg) > t.o.MaxErrorBytes {
		return msg[:t.o.MaxErrorBytes]
	}
	return msg
}
