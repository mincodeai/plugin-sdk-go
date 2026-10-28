package profiles_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/profiles"
	"github.com/mincodeai/plugin-sdk-go/rpctest"
)

func TestNormalizeMeta(t *testing.T) {
	got, err := profiles.NormalizeMeta(profiles.Meta{Group: " IM 项目 ", Environment: "prod"})
	if err != nil || got != (profiles.Meta{Group: "IM 项目", Environment: "prod"}) {
		t.Fatalf("NormalizeMeta = %+v, %v", got, err)
	}
	if got, err := profiles.NormalizeMeta(profiles.Meta{Group: "   ", Environment: " test "}); err != nil || got != (profiles.Meta{Environment: "test"}) {
		t.Fatalf("blank group = %+v, %v", got, err)
	}
	if _, err := profiles.NormalizeMeta(profiles.Meta{Group: strings.Repeat("组", 40)}); err != nil {
		t.Fatalf("40 runes: %v", err)
	}
	for _, c := range []struct {
		m     profiles.Meta
		field string
	}{
		{profiles.Meta{Group: strings.Repeat("组", 41)}, "group"},
		{profiles.Meta{Group: "a\nb"}, "group"},
		{profiles.Meta{Group: "a\tb"}, "group"},
		{profiles.Meta{Group: "a b"}, "group"},
		{profiles.Meta{Group: "a\xffb"}, "group"},
		{profiles.Meta{Environment: "production"}, "environment"},
		{profiles.Meta{Environment: "Prod"}, "environment"},
	} {
		_, err := profiles.NormalizeMeta(c.m)
		var me *profiles.MetaError
		if !errors.As(err, &me) || me.Field != c.field || me.Error() == "" {
			t.Errorf("NormalizeMeta(%+v) = %v, want MetaError on %s", c.m, err, c.field)
		}
	}
	_, err = profiles.NormalizeMeta(profiles.Meta{Group: strings.Repeat("x", 41)})
	if err == nil || err.Error() != "分组名最多 40 个字符" {
		t.Fatalf("message = %v", err)
	}
}

func TestMetaStoreSetRenameDelete(t *testing.T) {
	fh := rpctest.NewFakeHost()
	ctx := fh.Context(context.Background())
	var s profiles.MetaStore
	if m, err := s.Load(ctx); err != nil || m == nil || len(m) != 0 {
		t.Fatalf("empty load = %v, %v", m, err)
	}
	for _, c := range []struct {
		id string
		m  profiles.Meta
	}{{"a", profiles.Meta{"IM", "prod"}}, {"b", profiles.Meta{" IM ", ""}}, {"c", profiles.Meta{"", "test"}}} {
		if _, err := s.Set(ctx, c.id, c.m); err != nil {
			t.Fatal(err)
		}
	}
	want := `{"version":1,"meta":{"a":{"group":"IM","environment":"prod"},"b":{"group":"IM"},"c":{"environment":"test"}}}`
	if got := string(fh.Storage["profileMeta"]); got != want {
		t.Fatalf("stored %s", got)
	}
	if _, ok := fh.Storage["profiles"]; ok {
		t.Fatal("profiles document touched")
	}
	m, err := s.RenameGroup(ctx, "IM", "Chat")
	if err != nil || m["a"].Group != "Chat" || m["b"].Group != "Chat" || m["c"].Group != "" {
		t.Fatalf("rename = %v, %v", m, err)
	}
	if m, _ := s.Load(ctx); m["a"].Group != "Chat" {
		t.Fatalf("rename not stored: %v", m)
	}
	// Group names are case-sensitive.
	if m, _ := s.RenameGroup(ctx, "chat", "X"); m["a"].Group != "Chat" {
		t.Fatalf("case-insensitive rename: %v", m)
	}
	m, err = s.RenameGroup(ctx, " Chat ", "")
	if err != nil || !reflect.DeepEqual(m, map[string]profiles.Meta{"a": {Environment: "prod"}, "c": {Environment: "test"}}) {
		t.Fatalf("delete group = %v, %v", m, err)
	}
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if m, _ := s.Load(ctx); !reflect.DeepEqual(m, map[string]profiles.Meta{"c": {Environment: "test"}}) {
		t.Fatalf("after delete = %v", m)
	}
	if m, err := s.Set(ctx, "c", profiles.Meta{Group: " ", Environment: ""}); err != nil || len(m) != 0 {
		t.Fatalf("empty set = %v, %v", m, err)
	}
	if got := string(fh.Storage["profileMeta"]); got != `{"version":1,"meta":{}}` {
		t.Fatalf("stored %s", got)
	}
	var me *profiles.MetaError
	if _, err := s.Set(ctx, "d", profiles.Meta{Environment: "live"}); !errors.As(err, &me) {
		t.Fatalf("invalid set: %v", err)
	}
	if _, err := s.RenameGroup(ctx, "  ", "x"); !errors.As(err, &me) {
		t.Fatalf("empty from: %v", err)
	}
	if _, err := s.RenameGroup(ctx, "x", strings.Repeat("y", 41)); !errors.As(err, &me) {
		t.Fatalf("long to: %v", err)
	}

	// Deleting ids without metadata does not write.
	fh.Calls = nil
	if err := s.Delete(ctx, "zz"); err != nil || len(fh.Calls) != 1 {
		t.Fatalf("no-op delete calls = %q, %v", fh.Calls, err)
	}

	custom := profiles.MetaStore{Key: "groups"}
	if _, err := custom.Set(ctx, "a", profiles.Meta{Group: "g"}); err != nil || string(fh.Storage["groups"]) != `{"version":1,"meta":{"a":{"group":"g"}}}` {
		t.Fatalf("custom key = %s, %v", fh.Storage["groups"], err)
	}

	fh.Fail = func(string, json.RawMessage) error { return &host.Error{Code: -32001} }
	if _, err := s.Load(ctx); profiles.Classify(err) != profiles.Denied {
		t.Fatalf("denied: %v", err)
	}
	fh.Fail = nil
	unconfirmed := host.CallerFunc(func(_ context.Context, method string, _ any) (json.RawMessage, error) {
		if method == "host.storage.get" {
			return json.RawMessage("null"), nil
		}
		return json.RawMessage("false"), nil
	})
	if _, err := s.Set(host.WithCaller(ctx, unconfirmed), "a", profiles.Meta{Group: "g"}); err != profiles.ErrNotConfirmed {
		t.Fatalf("unconfirmed: %v", err)
	}
	if _, err := s.Load(context.Background()); err != host.ErrUnavailable {
		t.Fatalf("no host: %v", err)
	}
}

func knownIDs(ids ...string) func(context.Context) ([]string, error) {
	return func(context.Context) ([]string, error) { return ids, nil }
}

func handle(t *testing.T, s *profiles.MetaStore, ctx context.Context, action, payload string, known func(context.Context) ([]string, error)) (string, error) {
	t.Helper()
	res, handled, err := s.Handle(ctx, action, json.RawMessage(payload), known)
	if !handled {
		t.Fatalf("%s not handled", action)
	}
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	return string(b), nil
}

func TestMetaStoreHandle(t *testing.T) {
	fh := rpctest.NewFakeHost()
	ctx := fh.Context(context.Background())
	var s profiles.MetaStore
	known := knownIDs("a", "b")
	if res, handled, err := s.Handle(ctx, "listProfiles", nil, known); handled || res != nil || err != nil || len(fh.Calls) != 0 {
		t.Fatalf("unknown action = %v %v %v", res, handled, err)
	}
	got, err := handle(t, &s, ctx, "profileMeta", `{}`, known)
	if err != nil || got != `{"meta":{},"prefs":{"collapsed":[],"width":240,"recent":[]}}` {
		t.Fatalf("empty profileMeta = %s, %v", got, err)
	}
	if got, err := handle(t, &s, ctx, "profileMeta", ``, known); err != nil || !strings.HasPrefix(got, `{"meta":{}`) {
		t.Fatalf("absent payload = %s, %v", got, err)
	}
	got, err = handle(t, &s, ctx, "setProfileMeta", `{"id":"a","group":" IM ","environment":"prod"}`, known)
	if err != nil || got != `{"meta":{"a":{"group":"IM","environment":"prod"}}}` {
		t.Fatalf("setProfileMeta = %s, %v", got, err)
	}
	// Metadata of profiles deleted elsewhere stays stored but is not returned.
	if _, err := s.Set(ctx, "gone", profiles.Meta{Group: "IM"}); err != nil {
		t.Fatal(err)
	}
	got, err = handle(t, &s, ctx, "profileMeta", `{}`, known)
	if err != nil || !strings.HasPrefix(got, `{"meta":{"a":{"group":"IM","environment":"prod"}},`) {
		t.Fatalf("filtered profileMeta = %s, %v", got, err)
	}
	var me *profiles.MetaError
	if _, err := handle(t, &s, ctx, "setProfileMeta", `{"id":"zz","group":"IM"}`, known); !errors.As(err, &me) || err.Error() != "连接不存在" {
		t.Fatalf("unknown id: %v", err)
	}
	if _, err := handle(t, &s, ctx, "setProfileMeta", `{"id":"a","environment":"live"}`, known); !errors.As(err, &me) || me.Field != "environment" {
		t.Fatalf("bad environment: %v", err)
	}
	for _, c := range [][2]string{
		{"profileMeta", `{"x":1}`},
		{"setProfileMeta", `{"id":"a","group":"IM","color":"red"}`},
		{"setProfileMeta", `{"id":"a","group":7}`},
		{"setProfileMeta", `[]`},
		{"renameProfileGroup", `{"from":"IM","to":"x","y":1}`},
		{"setConnectionPrefs", `{"width":240,"theme":"dark"}`},
	} {
		if _, err := handle(t, &s, ctx, c[0], c[1], known); !errors.As(err, &me) {
			t.Errorf("%s %s: %v", c[0], c[1], err)
		}
	}
	got, err = handle(t, &s, ctx, "renameProfileGroup", `{"from":"IM","to":"Chat"}`, known)
	if err != nil || got != `{"meta":{"a":{"group":"Chat","environment":"prod"}}}` {
		t.Fatalf("rename = %s, %v", got, err)
	}
	if m, _ := s.Load(ctx); m["gone"].Group != "Chat" {
		t.Fatalf("rename skipped unknown ids: %v", m)
	}
	if _, err := handle(t, &s, ctx, "renameProfileGroup", `{"from":" ","to":"x"}`, known); !errors.As(err, &me) {
		t.Fatalf("empty from: %v", err)
	}
	knownErr := errors.New("boom")
	if _, _, err := s.Handle(ctx, "profileMeta", nil, func(context.Context) ([]string, error) { return nil, knownErr }); err != knownErr {
		t.Fatalf("known error: %v", err)
	}

	limited := profiles.MetaStore{Key: "limited", MaxEntries: 2}
	all := knownIDs("a", "b", "c")
	for _, id := range []string{"a", "b"} {
		if _, err := handle(t, &limited, ctx, "setProfileMeta", `{"id":"`+id+`","group":"g"}`, all); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := handle(t, &limited, ctx, "setProfileMeta", `{"id":"c","group":"g"}`, all); !errors.As(err, &me) {
		t.Fatalf("over MaxEntries: %v", err)
	}
	// Updating or clearing an existing entry is still allowed at the limit.
	if _, err := handle(t, &limited, ctx, "setProfileMeta", `{"id":"a","group":"h"}`, all); err != nil {
		t.Fatalf("update at limit: %v", err)
	}
	if _, err := handle(t, &limited, ctx, "setProfileMeta", `{"id":"c"}`, all); err != nil {
		t.Fatalf("empty set at limit: %v", err)
	}
}

func TestMetaStoreLoadIgnoresGarbage(t *testing.T) {
	fh := rpctest.NewFakeHost()
	ctx := fh.Context(context.Background())
	var s profiles.MetaStore
	for _, raw := range []string{`"nope"`, `{"version":2,"meta":{}}`, `{"version":2,"meta":{"a":{"group":"g"}}}`, `[1]`, `{"version":1,"meta":[1]}`, `nope`} {
		fh.Storage["profileMeta"] = json.RawMessage(raw)
		if m, err := s.Load(ctx); err != nil || m == nil || len(m) != 0 {
			t.Errorf("Load(%s) = %v, %v", raw, m, err)
		}
	}
	// Invalid entries are dropped; valid ones survive and are normalized.
	fh.Storage["profileMeta"] = json.RawMessage(`{"version":1,"meta":{"a":{"group":" g "},"b":{"environment":"live"},"c":7,"d":{}}}`)
	if m, err := s.Load(ctx); err != nil || !reflect.DeepEqual(m, map[string]profiles.Meta{"a": {Group: "g"}}) {
		t.Fatalf("partial = %v, %v", m, err)
	}
}

func TestConnectionPrefs(t *testing.T) {
	fh := rpctest.NewFakeHost()
	ctx := fh.Context(context.Background())
	var s profiles.MetaStore
	known := knownIDs("a", "b", "c", "d", "e", "f", "g")
	prefs := func() string {
		t.Helper()
		got, err := handle(t, &s, ctx, "profileMeta", `{}`, known)
		if err != nil {
			t.Fatal(err)
		}
		var v struct {
			Prefs json.RawMessage `json:"prefs"`
		}
		_ = json.Unmarshal([]byte(got), &v)
		return string(v.Prefs)
	}
	if got := prefs(); got != `{"collapsed":[],"width":240,"recent":[]}` {
		t.Fatalf("default prefs = %s", got)
	}
	got, err := handle(t, &s, ctx, "setConnectionPrefs", `{"collapsed":[" IM ","","Ops","IM"],"recent":["b","a"]}`, known)
	if err != nil || got != `{"prefs":{"collapsed":["IM","","Ops"],"width":240,"recent":["b","a"]}}` {
		t.Fatalf("set prefs = %s, %v", got, err)
	}
	got, err = handle(t, &s, ctx, "setConnectionPrefs", `{"width":300}`, known)
	if err != nil || got != `{"prefs":{"collapsed":["IM","","Ops"],"width":300,"recent":["b","a"]}}` {
		t.Fatalf("width only = %s, %v", got, err)
	}
	if got := prefs(); got != `{"collapsed":["IM","","Ops"],"width":300,"recent":["b","a"]}` {
		t.Fatalf("stored prefs = %s", got)
	}
	var me *profiles.MetaError
	long := `["` + strings.Repeat(`g","`, 100) + `g"]`
	for _, payload := range []string{`{"width":100}`, `{"width":361}`, `{"width":179}`, `{"collapsed":` + long + `}`, `{"collapsed":["a\nb"]}`, `{"width":250.5}`} {
		if _, err := handle(t, &s, ctx, "setConnectionPrefs", payload, known); !errors.As(err, &me) {
			t.Errorf("%s: %v", payload, err)
		}
	}
	if _, err := handle(t, &s, ctx, "setConnectionPrefs", `{"width":180}`, known); err != nil {
		t.Fatalf("width 180: %v", err)
	}
	got, err = handle(t, &s, ctx, "setConnectionPrefs", `{"width":360,"recent":["zz","a","b","a","c","d","e","f","g"]}`, known)
	if err != nil || got != `{"prefs":{"collapsed":["IM","","Ops"],"width":360,"recent":["a","b","c","d","e"]}}` {
		t.Fatalf("recent = %s, %v", got, err)
	}
	if _, err := s.RenameGroup(ctx, "IM", "Chat"); err != nil {
		t.Fatal(err)
	}
	if got := prefs(); got != `{"collapsed":["Chat","","Ops"],"width":360,"recent":["a","b","c","d","e"]}` {
		t.Fatalf("after rename = %s", got)
	}
	if _, err := s.RenameGroup(ctx, "Ops", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if got := prefs(); got != `{"collapsed":["Chat",""],"width":360,"recent":["b","c","d","e"]}` {
		t.Fatalf("after delete = %s", got)
	}
	// Recent ids of profiles deleted elsewhere are hidden.
	known = knownIDs("c", "e")
	if got := prefs(); got != `{"collapsed":["Chat",""],"width":360,"recent":["c","e"]}` {
		t.Fatalf("filtered recent = %s", got)
	}
	want := `{"version":1,"meta":{},"prefs":{"collapsed":["Chat",""],"width":360,"recent":["b","c","d","e"]}}`
	if got := string(fh.Storage["profileMeta"]); got != want {
		t.Fatalf("stored %s", got)
	}
}
