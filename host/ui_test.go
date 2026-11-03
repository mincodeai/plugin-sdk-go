package host

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestUIHelpersCallGenericHostMethods(t *testing.T) {
	var calls []string
	var params []any
	caller := CallerFunc(func(_ context.Context, method string, p any) (json.RawMessage, error) {
		calls = append(calls, method)
		params = append(params, p)
		switch method {
		case "host.quickPick":
			return json.RawMessage(`{"cancelled":false,"ids":["prod"]}`), nil
		case "host.inputBox":
			return json.RawMessage(`{"cancelled":true,"value":""}`), nil
		}
		return json.RawMessage(`{"requested":true}`), nil
	})
	ctx := context.Background()
	if err := OpenEditor(ctx, caller, EditorLocation{Path: "main.go", Line: 3}); err != nil {
		t.Fatal(err)
	}
	if err := OpenView(ctx, caller, ViewRequest{PluginID: "report", ViewID: "main", Entry: "draft", Params: map[string]any{"week": "40"}}); err != nil {
		t.Fatal(err)
	}
	if err := OpenDiff(ctx, caller, DiffRequest{Title: "a", Left: DiffSide{Content: "x"}, Right: DiffSide{Content: "y"}}); err != nil {
		t.Fatal(err)
	}
	if err := OpenTerminal(ctx, caller, TerminalRequest{Command: "make test"}); err != nil {
		t.Fatal(err)
	}
	ids, cancelled, err := QuickPick(ctx, caller, QuickPickRequest{Items: []QuickPickItem{{ID: "prod", Label: "Prod"}}})
	if err != nil || cancelled || len(ids) != 1 || ids[0] != "prod" {
		t.Fatalf("%v %v %v", ids, cancelled, err)
	}
	if _, cancelled, err := InputBox(ctx, caller, InputBoxRequest{Title: "Tag"}); err != nil || !cancelled {
		t.Fatalf("%v %v", cancelled, err)
	}
	if err := OpenProject(ctx, caller, "/tmp/theme"); err != nil {
		t.Fatal(err)
	}
	want := []string{"host.editor.open", "host.view.open", "host.diff.open", "host.terminal.open", "host.quickPick", "host.inputBox", "host.project.open"}
	for i, method := range want {
		if calls[i] != method {
			t.Fatalf("calls %v", calls)
		}
	}
	if raw, _ := json.Marshal(params[0]); string(raw) != `{"path":"main.go","line":3}` {
		t.Fatalf("editor params %s", raw)
	}
	if OpenEditor(ctx, caller, EditorLocation{}) == nil || OpenView(ctx, caller, ViewRequest{}) == nil || OpenDiff(ctx, caller, DiffRequest{}) == nil {
		t.Fatal("missing required fields accepted")
	}
	if OpenEditor(ctx, nil, EditorLocation{Path: "a"}) != ErrUnavailable {
		t.Fatal("nil caller")
	}
}

func TestViewRequestRaiseSerialization(t *testing.T) {
	plain, _ := json.Marshal(ViewRequest{ViewID: "main"})
	if string(plain) != `{"viewId":"main"}` {
		t.Fatalf("plain %s", plain)
	}
	raised, _ := json.Marshal(ViewRequest{ViewID: "main", Raise: true, Activation: "tok"})
	if string(raised) != `{"viewId":"main","raise":true,"activation":"tok"}` {
		t.Fatalf("raise %s", raised)
	}
	var sent any
	caller := CallerFunc(func(_ context.Context, _ string, p any) (json.RawMessage, error) {
		sent = p
		return json.RawMessage(`{"requested":true,"raised":true}`), nil
	})
	if err := OpenView(context.Background(), caller, ViewRequest{ViewID: "main", Raise: true}); err == nil || sent != nil {
		t.Fatal("raise without activation was sent")
	}
	if err := OpenView(context.Background(), caller, ViewRequest{ViewID: "main", Raise: true, Activation: "tok"}); err != nil || sent == nil {
		t.Fatalf("raise: %v", err)
	}
}

func TestTriggerFrom(t *testing.T) {
	for payload, want := range map[string]Trigger{
		`{"trigger":{"type":"globalShortcut","id":"capture"},"mode":"region"}`: {Type: "globalShortcut", ID: "capture"},
		`"{\"trigger\":{\"type\":\"globalShortcut\",\"id\":\"full\"}}"`:        {Type: "globalShortcut", ID: "full"},
	} {
		got, ok := TriggerFrom(json.RawMessage(payload))
		if !ok || got != want {
			t.Fatalf("%s: %+v %v", payload, got, ok)
		}
	}
	for _, payload := range []string{``, `null`, `{}`, `{"mode":"region"}`, `{"trigger":"x"}`, `{"trigger":{"type":"","id":"x"}}`, `{"trigger":{"type":"globalShortcut"}}`, `[1]`, `not json`} {
		if got, ok := TriggerFrom(json.RawMessage(payload)); ok {
			t.Fatalf("%s: %+v", payload, got)
		}
	}
}

func TestPluginCallHelpers(t *testing.T) {
	caller := CallerFunc(func(_ context.Context, method string, p any) (json.RawMessage, error) {
		switch method {
		case "host.plugins.list":
			return json.RawMessage(`{"plugins":[{"id":"report","name":"Report","actions":["activity"],"granted":true}]}`), nil
		case "host.plugins.invoke":
			raw, _ := json.Marshal(p)
			if string(raw) != `{"action":"activity","payload":{},"pluginId":"report"}` {
				t.Errorf("params %s", raw)
			}
			return json.RawMessage(`{"result":{"items":1}}`), nil
		}
		return nil, nil
	})
	plugins, err := ListPlugins(context.Background(), caller)
	if err != nil || len(plugins) != 1 || !plugins[0].Granted {
		t.Fatalf("%+v %v", plugins, err)
	}
	result, err := InvokePlugin(context.Background(), caller, PluginCall{PluginID: "report", Action: "activity"})
	if err != nil || string(result) != `{"items":1}` {
		t.Fatalf("%s %v", result, err)
	}
	if _, err := InvokePlugin(context.Background(), caller, PluginCall{Action: "x"}); err == nil {
		t.Fatal("missing plugin id accepted")
	}
}

func TestDatabaseSchemaHelpers(t *testing.T) {
	var sent []string
	caller := CallerFunc(func(_ context.Context, method string, p any) (json.RawMessage, error) {
		raw, _ := json.Marshal(p)
		sent = append(sent, method+" "+string(raw))
		switch method {
		case "host.database.connections":
			return json.RawMessage(`{"connections":[{"id":"c1","name":"Orders","kind":"mysql","granted":true}]}`), nil
		case "host.database.tables":
			return json.RawMessage(`{"databases":["shop"],"database":"shop","tables":[{"name":"orders","kind":"table"}]}`), nil
		case "host.database.describe":
			return json.RawMessage(`{"tables":[{"name":"orders","columns":[{"name":"id","dataType":"bigint","primaryKey":true,"defaultValue":"0"}],"foreignKeys":[{"name":"fk","columns":["uid"],"referencedTable":"users","referencedColumns":["id"]}]}]}`), nil
		}
		return nil, nil
	})
	ctx := context.Background()
	connections, err := ListDatabaseConnections(ctx, caller)
	if err != nil || len(connections) != 1 || !connections[0].Granted {
		t.Fatalf("%+v %v", connections, err)
	}
	listing, err := ListDatabaseTables(ctx, caller, DatabaseTablesRequest{ConnectionID: "c1"})
	if err != nil || listing.Database != "shop" || listing.Tables[0].Name != "orders" {
		t.Fatalf("%+v %v", listing, err)
	}
	tables, err := DescribeDatabaseTables(ctx, caller, DatabaseDescribeRequest{ConnectionID: "c1", Database: "shop", Tables: []string{"orders"}})
	if err != nil || len(tables) != 1 || !tables[0].Columns[0].PrimaryKey || *tables[0].Columns[0].DefaultValue != "0" || tables[0].ForeignKeys[0].ReferencedTable != "users" {
		t.Fatalf("%+v %v", tables, err)
	}
	if sent[1] != `host.database.tables {"connectionId":"c1"}` {
		t.Fatalf("sent %v", sent)
	}
	if _, err := DescribeDatabaseTables(ctx, caller, DatabaseDescribeRequest{ConnectionID: "c1", Tables: []string{"x"}}); err == nil {
		t.Fatal("missing database accepted")
	}
	if _, err := ListDatabaseTables(ctx, nil, DatabaseTablesRequest{ConnectionID: "c1"}); err != ErrUnavailable {
		t.Fatal("nil caller")
	}
}

func TestClientHonoursPerCallTimeout(t *testing.T) {
	sent := make(chan []byte, 2)
	client := NewClient(func(line []byte) error { sent <- line; return nil }, Options{Timeout: 10 * time.Millisecond})
	start := time.Now()
	if _, err := client.Call(context.Background(), "host.quickPick", nil); err == nil {
		t.Fatal("default timeout did not fire")
	}
	if time.Since(start) > time.Second {
		t.Fatal("default timeout too slow")
	}
	<-sent
	done := make(chan error, 1)
	go func() {
		_, err := client.Call(WithCallTimeout(context.Background(), 2*time.Second), "host.quickPick", nil)
		done <- err
	}()
	line := <-sent
	time.Sleep(100 * time.Millisecond) // well past the 10 ms default
	var request struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(line, &request)
	raw, _ := json.Marshal(request.ID)
	if !client.Deliver(raw, json.RawMessage(`{"cancelled":true}`), nil) {
		t.Fatal("call no longer pending after the default timeout")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
