package host

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// serviceHost is a scripted host for the services helpers: it records each
// call's method, params and host-call timeout, and answers from reply.
type serviceHost struct {
	methods  []string
	params   []string
	timeouts []time.Duration
	reply    func(method string, params map[string]any) (json.RawMessage, error)
}

func (h *serviceHost) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, _ := json.Marshal(params)
	h.methods = append(h.methods, method)
	h.params = append(h.params, string(raw))
	wait, _ := ctx.Value(callTimeoutKey{}).(time.Duration)
	h.timeouts = append(h.timeouts, wait)
	var decoded map[string]any
	_ = json.Unmarshal(raw, &decoded)
	return h.reply(method, decoded)
}

func TestInvokeServiceSendsCallAndCoversApprovalAndExecution(t *testing.T) {
	h := &serviceHost{reply: func(string, map[string]any) (json.RawMessage, error) {
		return json.RawMessage(`{"provider":{"pluginId":"text-tools","entryId":"reverse"},"output":"olleh"}`), nil
	}}
	text := "hello"
	result, err := InvokeService(context.Background(), h, ServiceCall{
		Service:   "demo.text.reverse",
		Provider:  &ServiceProviderRef{PluginID: "text-tools", EntryID: "reverse"},
		Text:      &text,
		Params:    map[string]any{"keepLines": true},
		TimeoutMs: 10000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(h.methods) != 1 || h.methods[0] != "host.services.invoke" {
		t.Fatalf("methods %v", h.methods)
	}
	if want := `{"input":"hello","params":{"keepLines":true},"provider":{"pluginId":"text-tools","entryId":"reverse"},"service":"demo.text.reverse","timeoutMs":10000}`; h.params[0] != want {
		t.Fatalf("params %s", h.params[0])
	}
	// Approval (10 s) + the longest entry timeout (300 s) + the reply margin.
	if want := 10*time.Second + 300*time.Second + 5*time.Second; h.timeouts[0] != want {
		t.Fatalf("timeout %v, want %v", h.timeouts[0], want)
	}
	if result.Provider.PluginID != "text-tools" || result.Provider.EntryID != "reverse" {
		t.Fatalf("provider %+v", result.Provider)
	}
	if got, err := result.Text(); err != nil || got != "olleh" {
		t.Fatalf("text %q %v", got, err)
	}

	// Files, no params, default approval, an execution bound from the caller.
	h.reply = func(string, map[string]any) (json.RawMessage, error) {
		return json.RawMessage(`{"provider":{"pluginId":"p","entryId":"copy"},"output":[{"token":"sf_1","name":"a.png","mime":"image/png","size":3}]}`), nil
	}
	result, err = InvokeService(context.Background(), h, ServiceCall{
		Service:       "demo.file.copy",
		Files:         []ServiceFileSource{{Source: "workspace", Path: "img/a.png"}, {Source: "upload", Token: "sf_up"}},
		ExecTimeoutMs: 2000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"input":[{"source":"workspace","path":"img/a.png"},{"source":"upload","token":"sf_up"}],"service":"demo.file.copy"}`; h.params[1] != want {
		t.Fatalf("params %s", h.params[1])
	}
	if want := 25*time.Second + 2*time.Second + 5*time.Second; h.timeouts[1] != want {
		t.Fatalf("timeout %v, want %v", h.timeouts[1], want)
	}
	files, err := result.Files()
	if err != nil || len(files) != 1 || files[0].Token != "sf_1" || files[0].Name != "a.png" || files[0].Mime != "image/png" || files[0].Size != 3 {
		t.Fatalf("files %+v %v", files, err)
	}

	if _, err := InvokeService(context.Background(), h, ServiceCall{Service: "a.b", Text: &text, Files: []ServiceFileSource{{Source: "upload", Token: "sf_1"}}}); err == nil {
		t.Fatal("text and files accepted together")
	}
	if _, err := InvokeService(context.Background(), h, ServiceCall{}); err == nil {
		t.Fatal("missing service accepted")
	}
	if _, err := InvokeService(context.Background(), nil, ServiceCall{Service: "a.b"}); err != ErrUnavailable {
		t.Fatalf("nil caller: %v", err)
	}

	h.reply = func(string, map[string]any) (json.RawMessage, error) {
		return nil, &Error{Code: -32000, Message: "too_large: input is 9000 bytes", Data: json.RawMessage(`{"code":"too_large"}`)}
	}
	if _, err := InvokeService(context.Background(), h, ServiceCall{Service: "a.b", Text: &text}); ServiceErrorCode(err) != "too_large" {
		t.Fatalf("error code of %v", err)
	}
}

func TestListStartCancelAndFileHelpersSendHostMethods(t *testing.T) {
	h := &serviceHost{reply: func(method string, _ map[string]any) (json.RawMessage, error) {
		switch method {
		case "host.services.list":
			return json.RawMessage(`{"services":[{"service":"demo.text.reverse","providers":[{"pluginId":"p","pluginName":"P","entryId":"reverse","title":"Reverse","input":{"kind":"text","maxBytes":8192},"output":{"kind":"text"},"risk":"read","isDefault":true}]}]}`), nil
		case "host.services.start":
			return json.RawMessage(`{"callId":"sc_1","provider":{"pluginId":"p","entryId":"reverse"}}`), nil
		case "host.services.cancel":
			return json.RawMessage(`{"callId":"sc_1","state":"cancelled","provider":{"pluginId":"p","entryId":"reverse"}}`), nil
		case "host.services.file.save":
			return json.RawMessage(`{"path":"out/a.png","size":3}`), nil
		}
		return json.RawMessage(`true`), nil
	}}
	ctx := context.Background()
	services, err := ListServices(ctx, h, "demo.text.reverse")
	if err != nil || len(services) != 1 || services[0].Service != "demo.text.reverse" || len(services[0].Providers) != 1 {
		t.Fatalf("%+v %v", services, err)
	}
	if p := services[0].Providers[0]; p.PluginID != "p" || p.EntryID != "reverse" || p.Input.Kind != "text" || p.Input.MaxBytes != 8192 || !p.IsDefault || p.Risk != "read" {
		t.Fatalf("provider %+v", p)
	}
	callID, err := StartService(ctx, h, ServiceCall{Service: "demo.wait", Params: map[string]any{"seconds": 3}})
	if err != nil || callID != "sc_1" {
		t.Fatalf("%q %v", callID, err)
	}
	if err := CancelService(ctx, h, "sc_1"); err != nil {
		t.Fatal(err)
	}
	if err := SaveServiceFile(ctx, h, "sf_1", "out/a.png"); err != nil {
		t.Fatal(err)
	}
	if err := ReleaseServiceFile(ctx, h, "sf_1"); err != nil {
		t.Fatal(err)
	}
	wantMethods := []string{"host.services.list", "host.services.start", "host.services.cancel", "host.services.file.save", "host.services.file.release"}
	wantParams := []string{`{"service":"demo.text.reverse"}`, `{"params":{"seconds":3},"service":"demo.wait"}`, `{"callId":"sc_1"}`, `{"path":"out/a.png","token":"sf_1"}`, `{"token":"sf_1"}`}
	for i := range wantMethods {
		if h.methods[i] != wantMethods[i] || h.params[i] != wantParams[i] {
			t.Fatalf("call %d: %s %s", i, h.methods[i], h.params[i])
		}
	}
	// start waits for the approval only.
	if h.timeouts[1] != 25*time.Second+5*time.Second {
		t.Fatalf("start timeout %v", h.timeouts[1])
	}
	if StartService(ctx, h, ServiceCall{}); len(h.methods) != len(wantMethods) {
		t.Fatal("start without a service reached the host")
	}
	for _, err := range []error{CancelService(ctx, h, ""), SaveServiceFile(ctx, h, "", "a"), SaveServiceFile(ctx, h, "sf_1", ""), ReleaseServiceFile(ctx, h, "")} {
		if err == nil {
			t.Fatal("missing argument accepted")
		}
	}
}

func TestUploadServiceFileSendsContiguousChunks(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 1258291/16+1)[:1258291] // 1.2 MiB
	var received bytes.Buffer
	h := &serviceHost{reply: func(method string, p map[string]any) (json.RawMessage, error) {
		switch method {
		case "host.services.file.upload.begin":
			return json.RawMessage(`{"session":"ss_1","maxChunkBytes":524288}`), nil
		case "host.services.file.upload.append":
			chunk, err := base64.StdEncoding.DecodeString(p["data"].(string))
			if err != nil {
				return nil, err
			}
			if int(p["offset"].(float64)) != received.Len() {
				return nil, errors.New("offset gap")
			}
			received.Write(chunk)
			return json.RawMessage(`{"size":` + jsonInt(received.Len()) + `}`), nil
		case "host.services.file.upload.commit":
			return json.RawMessage(`{"token":"sf_up","name":"report.bin","mime":"application/octet-stream","size":1258291}`), nil
		}
		return nil, errors.New("unexpected " + method)
	}}
	token, err := UploadServiceFile(context.Background(), h, "report.bin", bytes.NewReader(data))
	if err != nil || token != "sf_up" {
		t.Fatalf("%q %v", token, err)
	}
	want := []string{"host.services.file.upload.begin", "host.services.file.upload.append", "host.services.file.upload.append", "host.services.file.upload.append", "host.services.file.upload.commit"}
	if strings.Join(h.methods, ",") != strings.Join(want, ",") {
		t.Fatalf("methods %v", h.methods)
	}
	offsets := []int{}
	for _, raw := range h.params[1:4] {
		var p struct {
			Session string `json:"session"`
			Offset  int    `json:"offset"`
		}
		_ = json.Unmarshal([]byte(raw), &p)
		if p.Session != "ss_1" {
			t.Fatalf("append %s", raw)
		}
		offsets = append(offsets, p.Offset)
	}
	if offsets[0] != 0 || offsets[1] != 524288 || offsets[2] != 1048576 {
		t.Fatalf("offsets %v", offsets)
	}
	if !bytes.Equal(received.Bytes(), data) {
		t.Fatal("uploaded bytes differ")
	}
	if h.params[4] != `{"name":"report.bin","session":"ss_1"}` {
		t.Fatalf("commit %s", h.params[4])
	}

	// A host that offers larger chunks still gets at most 512 KiB.
	h.methods, h.params, received = nil, nil, bytes.Buffer{}
	inner := h.reply
	h.reply = func(method string, p map[string]any) (json.RawMessage, error) {
		if method == "host.services.file.upload.begin" {
			return json.RawMessage(`{"session":"ss_1","maxChunkBytes":4194304}`), nil
		}
		return inner(method, p)
	}
	if _, err := UploadServiceFile(context.Background(), h, "report.bin", bytes.NewReader(data)); err != nil || len(h.methods) != 5 {
		t.Fatalf("%v %v", h.methods, err)
	}
	if _, err := UploadServiceFile(context.Background(), h, "", bytes.NewReader(data)); err == nil {
		t.Fatal("missing name accepted")
	}
}

func jsonInt(n int) string { raw, _ := json.Marshal(n); return string(raw) }

func TestReadAndCreateServiceFiles(t *testing.T) {
	data := bytes.Repeat([]byte{7}, 524288+10)
	var created bytes.Buffer
	h := &serviceHost{reply: func(method string, p map[string]any) (json.RawMessage, error) {
		switch method {
		case "host.services.file.read":
			offset, length := int(p["offset"].(float64)), int(p["length"].(float64))
			end := min(offset+length, len(data))
			offset = min(offset, end)
			raw, _ := json.Marshal(map[string]string{"data": base64.StdEncoding.EncodeToString(data[offset:end])})
			return raw, nil
		case "host.services.file.create.begin":
			return json.RawMessage(`{"session":"ss_2","maxChunkBytes":524288}`), nil
		case "host.services.file.create.append":
			chunk, _ := base64.StdEncoding.DecodeString(p["data"].(string))
			created.Write(chunk)
			return json.RawMessage(`{"size":1}`), nil
		case "host.services.file.create.commit":
			return json.RawMessage(`{"token":"sf_out","name":"copy.bin","mime":"application/octet-stream","size":524298}`), nil
		}
		return nil, errors.New("unexpected " + method)
	}}
	var out bytes.Buffer
	if err := ReadServiceFile(context.Background(), h, "sf_in", &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("read %d bytes", out.Len())
	}
	if h.params[0] != `{"length":524288,"offset":0,"token":"sf_in"}` || h.params[1] != `{"length":524288,"offset":524288,"token":"sf_in"}` {
		t.Fatalf("reads %v", h.params)
	}

	h.methods, h.params = nil, nil
	token, err := CreateServiceFile(context.Background(), h, "sc_1", "copy.bin", bytes.NewReader(data))
	if err != nil || token != "sf_out" {
		t.Fatalf("%q %v", token, err)
	}
	if h.params[0] != `{"callId":"sc_1","name":"copy.bin"}` || h.methods[len(h.methods)-1] != "host.services.file.create.commit" || h.params[len(h.params)-1] != `{"session":"ss_2"}` {
		t.Fatalf("create %v %v", h.methods, h.params)
	}
	if !bytes.Equal(created.Bytes(), data) {
		t.Fatal("created bytes differ")
	}
	if _, err := CreateServiceFile(context.Background(), h, "", "a", bytes.NewReader(nil)); err == nil {
		t.Fatal("missing call id accepted")
	}
	// An empty output file is begin + commit.
	h.methods = nil
	if _, err := CreateServiceFile(context.Background(), h, "sc_1", "empty.txt", bytes.NewReader(nil)); err != nil || len(h.methods) != 2 {
		t.Fatalf("%v %v", h.methods, err)
	}
}

func TestWaitServiceReportsProgressThenReturnsResult(t *testing.T) {
	restore := serviceWaitDelays
	serviceWaitDelays = []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond}
	defer func() { serviceWaitDelays = restore }()

	snapshots := []string{
		`{"callId":"sc_1","state":"running","provider":{"pluginId":"p","entryId":"wait"}}`,
		`{"callId":"sc_1","state":"running","provider":{"pluginId":"p","entryId":"wait"},"progress":{"completed":1,"total":3,"message":"1 of 3"},"rawProgress":{"completed":1,"total":3,"message":"1 of 3","stage":"a"}}`,
		`{"callId":"sc_1","state":"running","provider":{"pluginId":"p","entryId":"wait"},"progress":{"completed":1,"total":3,"message":"1 of 3"},"rawProgress":{"completed":1,"total":3,"message":"1 of 3","stage":"a"}}`,
		`{"callId":"sc_1","state":"running","provider":{"pluginId":"p","entryId":"wait"},"progress":{"completed":2,"total":3}}`,
		`{"callId":"sc_1","state":"succeeded","provider":{"pluginId":"p","entryId":"wait"},"output":"done"}`,
	}
	h := &serviceHost{reply: func(method string, _ map[string]any) (json.RawMessage, error) {
		if method != "host.services.status" {
			return nil, errors.New("unexpected " + method)
		}
		next := snapshots[0]
		snapshots = snapshots[1:]
		return json.RawMessage(next), nil
	}}
	var seen []ServiceProgress
	result, err := WaitService(context.Background(), h, "sc_1", func(p ServiceProgress) { seen = append(seen, p) })
	if err != nil {
		t.Fatal(err)
	}
	if text, _ := result.Text(); text != "done" || result.Provider.EntryID != "wait" {
		t.Fatalf("result %+v", result)
	}
	if len(h.methods) != 5 || h.params[0] != `{"callId":"sc_1"}` {
		t.Fatalf("polls %v %v", h.methods, h.params)
	}
	// Repeated snapshots report once.
	if len(seen) != 2 || *seen[0].Completed != 1 || *seen[0].Total != 3 || seen[0].Message != "1 of 3" || !strings.Contains(string(seen[0].Raw), `"stage":"a"`) || *seen[1].Completed != 2 {
		t.Fatalf("progress %+v", seen)
	}

	// A failed call is a *ServiceCallError with the code from "<code>: detail".
	h.reply = func(string, map[string]any) (json.RawMessage, error) {
		return json.RawMessage(`{"callId":"sc_2","state":"failed","provider":{"pluginId":"p","entryId":"wait"},"error":"timeout: the provider did not finish in time","unconfirmed":true}`), nil
	}
	_, err = WaitService(context.Background(), h, "sc_2", nil)
	var callErr *ServiceCallError
	if !errors.As(err, &callErr) || callErr.State != "failed" || !callErr.Unconfirmed || ServiceErrorCode(err) != "timeout" {
		t.Fatalf("failure %#v", err)
	}
	h.reply = func(string, map[string]any) (json.RawMessage, error) {
		return json.RawMessage(`{"callId":"sc_3","state":"cancelled","provider":{"pluginId":"p","entryId":"wait"}}`), nil
	}
	if _, err = WaitService(context.Background(), h, "sc_3", nil); ServiceErrorCode(err) != "cancelled" {
		t.Fatalf("cancelled %v", err)
	}

	// Cancelling ctx stops the wait (it does not cancel the call).
	h.reply = func(string, map[string]any) (json.RawMessage, error) {
		return json.RawMessage(`{"callId":"sc_4","state":"running","provider":{"pluginId":"p","entryId":"wait"}}`), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	h.methods = nil
	if _, err := WaitService(ctx, h, "sc_4", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ctx: %v", err)
	}
	for _, method := range h.methods {
		if method != "host.services.status" {
			t.Fatalf("wait sent %s", method)
		}
	}
	if _, err := WaitService(context.Background(), h, "", nil); err == nil {
		t.Fatal("missing call id accepted")
	}
}

func TestDecodeServiceRequestSurfaceContext(t *testing.T) {
	req, err := DecodeServiceRequest(json.RawMessage(`{"callId":"sc_4","service":"demo.file.inspect","entry":"inspect","input":[{"token":"sf_1","name":"response.json","mime":"application/json","size":2}],"params":{},"caller":{"kind":"host","surface":"httpResponse"},"surfaceContext":{"response":{"status":200,"headers":[["Content-Type","application/json"]],"headersRedacted":1}}}`))
	if err != nil {
		t.Fatal(err)
	}
	var context struct {
		Response struct {
			Status          int        `json:"status"`
			Headers         [][]string `json:"headers"`
			HeadersRedacted int        `json:"headersRedacted"`
		} `json:"response"`
	}
	if err := json.Unmarshal(req.SurfaceContext, &context); err != nil || context.Response.Status != 200 || len(context.Response.Headers) != 1 || context.Response.HeadersRedacted != 1 || req.Caller.Surface != "httpResponse" {
		t.Fatalf("%+v %s %v", req, req.SurfaceContext, err)
	}
	for _, payload := range []string{
		`{"callId":"sc_5","service":"demo.text.reverse","entry":"reverse","input":"abc","caller":{"kind":"plugin","pluginId":"consumer"}}`,
		`{"callId":"sc_5","service":"demo.text.reverse","entry":"reverse","input":"abc","caller":{"kind":"host","surface":"palette"},"surfaceContext":null}`,
	} {
		req, err := DecodeServiceRequest(json.RawMessage(payload))
		if err != nil || req.SurfaceContext != nil {
			t.Fatalf("%s: %s %v", payload, req.SurfaceContext, err)
		}
	}
	if _, err := DecodeServiceRequest(json.RawMessage(`{"callId":"sc_6","service":"a.b","entry":"e","surfaceContext":[1]}`)); err == nil {
		t.Fatal("accepted a surfaceContext that is not an object")
	}
}

func TestDecodeServiceRequestAndResults(t *testing.T) {
	req, err := DecodeServiceRequest(json.RawMessage(`{"callId":"sc_1","service":"demo.text.reverse","entry":"reverse","input":"abc","params":{"keepLines":true},"caller":{"kind":"plugin","pluginId":"consumer"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.CallID != "sc_1" || req.Service != "demo.text.reverse" || req.Entry != "reverse" || req.Text != "abc" || req.Files != nil || string(req.Params) != `{"keepLines":true}` || req.Caller.Kind != "plugin" || req.Caller.PluginID != "consumer" {
		t.Fatalf("%+v", req)
	}
	req, err = DecodeServiceRequest(json.RawMessage(`{"callId":"sc_2","service":"demo.file.copy","entry":"copy","input":[{"token":"sf_1","name":"a.png","mime":"image/png","size":12}],"params":{},"caller":{"kind":"host","surface":"fileMenu"}}`))
	if err != nil || len(req.Files) != 1 || req.Files[0].Token != "sf_1" || req.Files[0].Size != 12 || req.Caller.Surface != "fileMenu" {
		t.Fatalf("%+v %v", req, err)
	}
	req, err = DecodeServiceRequest(json.RawMessage(`{"callId":"sc_3","service":"demo.wait","entry":"wait","params":{"seconds":2},"caller":{"kind":"host","surface":"palette"}}`))
	if err != nil || req.Text != "" || req.Files != nil {
		t.Fatalf("%+v %v", req, err)
	}
	for _, bad := range []string{`[]`, `{"service":"a.b","entry":"e"}`, `{"callId":"sc_1","service":"a.b","entry":"e","input":3}`} {
		if _, err := DecodeServiceRequest(json.RawMessage(bad)); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
	checks := []struct {
		value any
		want  string
	}{
		{TextResult("olleh"), `{"text":"olleh"}`},
		{JSONResult(map[string]int{"n": 1}), `{"json":{"n":1}}`},
		{FilesResult("sf_1", "sf_2"), `{"files":["sf_1","sf_2"]}`},
		{FilesResult(), `{"files":[]}`},
	}
	for _, check := range checks {
		if raw, _ := json.Marshal(check.value); string(raw) != check.want {
			t.Fatalf("%s, want %s", raw, check.want)
		}
	}
}
