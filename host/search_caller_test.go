package host_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/host"
)

func TestSearchCallerWireTypes(t *testing.T) {
	wire := `{"items":[{"id":"a","title":"Main","target":{"type":"file","path":"src/main.go","line":12,"column":3}}],"dropped":0,"truncated":false,"fileScope":{"workspaceId":"w","root":"/project","revision":"9007199254740993"},"providerRuntime":{"instance":"18446744073709551615","generation":"1"}}`
	var response host.SearchResponse
	if err := json.Unmarshal([]byte(wire), &response); err != nil {
		t.Fatal(err)
	}
	if response.FileScope.Revision != "9007199254740993" || response.ProviderRuntime.Instance != "18446744073709551615" || response.ProviderRuntime.Generation != "1" {
		t.Fatalf("lost opaque identity: %+v", response)
	}
	if *response.Items[0].Target.Line != 12 || *response.Items[0].Target.Column != 3 {
		t.Fatal("lost target position")
	}
	var batch host.WorkspaceFileBatch
	if err := json.Unmarshal([]byte(`{"scopeRevision":"9007199254740993","origin":"watcher","rescanRequired":false,"renames":[{"oldPath":"old.go","newPath":"new.go"}]}`), &batch); err != nil {
		t.Fatal(err)
	}
	if batch.ScopeRevision != "9007199254740993" || batch.Origin != "watcher" || batch.RescanRequired || batch.Renames[0].OldPath != "old.go" || batch.Renames[0].NewPath != "new.go" {
		t.Fatalf("batch: %+v", batch)
	}
	got := []string{host.EventFileCreated, host.EventFileDeleted, host.EventFileRenamed, host.EventRescanRequired, host.EventGitStatusChanged, host.EventAppLockChanged, host.EventAppSuspend, host.EventAppResume}
	want := []string{"workspace.fileCreated", "workspace.fileDeleted", "workspace.fileRenamed", "workspace.rescanRequired", "git.statusChanged", "app.lockChanged", "app.suspend", "app.resume"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v", got)
	}
}

func TestCallerOptionalDevelopment(t *testing.T) {
	for _, caller := range []string{
		`{"kind":"plugin","pluginId":"old.plugin"}`,
		`{"kind":"plugin","pluginId":"unknown.plugin","source":"unknown"}`,
		`{"kind":"plugin","pluginId":"local.plugin","source":"local","development":false,"version":"0.3.0"}`,
		`{"kind":"plugin","pluginId":"dev.plugin","source":"development","development":true}`,
		`{"kind":"host","surface":"selection"}`,
	} {
		request, err := host.DecodeServiceRequest(json.RawMessage(`{"callId":"sc_test","service":"demo.echo","entry":"echo","input":null,"params":{},"caller":` + caller + `}`))
		if err != nil {
			t.Fatal(err)
		}
		field := reflect.ValueOf(request.Caller).FieldByName("Development")
		if !field.IsValid() || field.Type() != reflect.TypeOf((*bool)(nil)) {
			t.Fatal("Development must be *bool, nil is unknown not false")
		}
		raw, err := json.Marshal(request.Caller)
		if err != nil {
			t.Fatal(err)
		}
		var got, want any
		json.Unmarshal(raw, &got)
		json.Unmarshal([]byte(caller), &want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("caller changed: %s, want %s", raw, caller)
		}
	}
}

func TestDescribeRequiresTopLevelObject(t *testing.T) {
	for _, raw := range []string{"null", "[]", "1", `"description"`, "false"} {
		t.Run(raw, func(t *testing.T) {
			caller := host.CallerFunc(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(raw), nil })
			if _, err := host.Describe(context.Background(), caller); err == nil {
				t.Fatal("non-object description accepted")
			}
		})
	}
	caller := host.CallerFunc(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	if description, err := host.Describe(context.Background(), caller); err != nil || len(description.Events) != 0 {
		t.Fatalf("legacy object: %+v %v", description, err)
	}
}
