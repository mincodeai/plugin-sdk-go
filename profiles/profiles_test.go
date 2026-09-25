package profiles_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/jsonx"
	"github.com/mincodeai/plugin-sdk-go/profiles"
	"github.com/mincodeai/plugin-sdk-go/rpctest"
)

func TestClassify(t *testing.T) {
	for err, want := range map[error]profiles.Failure{
		nil:                                 profiles.NotFailed,
		&host.Error{Code: -32001}:           profiles.Denied,
		&host.Error{Code: -32003}:           profiles.Denied,
		&host.Error{Code: -32002}:           profiles.Failed,
		context.DeadlineExceeded:            profiles.TimedOut,
		context.Canceled:                    profiles.TimedOut,
		host.ErrUnavailable:                 profiles.Unavailable,
		errors.New("write |1: broken pipe"): profiles.Unavailable,
	} {
		if got := profiles.Classify(err); got != want {
			t.Errorf("Classify(%v) = %v, want %v", err, got, want)
		}
	}
}

func TestStore(t *testing.T) {
	fh := rpctest.NewFakeHost()
	ctx := fh.Context(context.Background())
	var s profiles.Store
	list, err := s.Load(ctx)
	if err != nil || len(list) != 0 || list == nil {
		t.Fatalf("empty load = %v, %v", list, err)
	}
	if err := s.Save(ctx, []map[string]string{{"id": "a", "url": "xy"}}); err != nil {
		t.Fatal(err)
	}
	if got := string(fh.Storage["profiles"]); got != `{"version":1,"profiles":[{"id":"a","url":"xy"}]}` {
		t.Fatalf("stored %s", got)
	}
	list, err = s.Load(ctx)
	if err != nil || len(list) != 1 || !strings.Contains(string(list[0]), `"id":"a"`) {
		t.Fatalf("load = %s, %v", list, err)
	}
	// A document stored as a JSON string is unwrapped; foreign versions and garbage load empty.
	fh.Storage["profiles"] = json.RawMessage(`"{\"version\":1,\"profiles\":[1,2]}"`)
	if list, _ = s.Load(ctx); len(list) != 2 {
		t.Fatalf("string doc = %s", list)
	}
	for _, raw := range []string{`{"version":2,"profiles":[1]}`, `[1]`, `{"version":1}`, `"nope"`} {
		if got := profiles.DecodeDocument(json.RawMessage(raw), 1); len(got) != 0 {
			t.Errorf("DecodeDocument(%s) = %s", raw, got)
		}
	}
	custom := profiles.Store{Key: "conn.v2", Version: 2}
	if err := custom.Save(ctx, []int{}); err != nil || string(fh.Storage["conn.v2"]) != `{"version":2,"profiles":[]}` {
		t.Fatalf("custom = %s, %v", fh.Storage["conn.v2"], err)
	}
	if b, _ := custom.Encode([]int{1}); string(b) != `{"version":2,"profiles":[1]}` {
		t.Fatalf("Encode = %s", b)
	}

	fh.Fail = func(method string, _ json.RawMessage) error { return &host.Error{Code: -32001} }
	if _, err := s.Load(ctx); profiles.Classify(err) != profiles.Denied {
		t.Fatalf("denied load: %v", err)
	}
	fh.Fail = nil
	unconfirmed := host.CallerFunc(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage("false"), nil })
	if err := s.Save(host.WithCaller(ctx, unconfirmed), []int{}); err != profiles.ErrNotConfirmed {
		t.Fatalf("unconfirmed: %v", err)
	}
	if _, err := s.Load(context.Background()); err != host.ErrUnavailable {
		t.Fatalf("no host: %v", err)
	}
}

func TestSecrets(t *testing.T) {
	fh := rpctest.NewFakeHost()
	ctx := fh.Context(context.Background())
	k := profiles.Secrets{IDPattern: regexp.MustCompile(`^[a-z0-9-]+$`), Fields: []string{"password", "token"}}
	if k.Key("p1", "token") != "profile.p1.token" {
		t.Fatal("Key")
	}
	for key, ok := range map[string]bool{
		"profile.p1.password": true, "profile.p1.other": false, "profile.P1.token": false,
		"profile..token": false, "other.p1.token": false, "profile.p1": false,
	} {
		if _, _, got := k.Parse(key); got != ok {
			t.Errorf("Parse(%s) = %v", key, got)
		}
	}
	for _, c := range [][3]string{{"b", "token", "t"}, {"b", "password", "p"}, {"a", "token", "x"}, {"c", "token", "y"}} {
		if err := k.Set(ctx, c[0], c[1], c[2]); err != nil {
			t.Fatal(err)
		}
	}
	fh.Secrets["unrelated"] = "z"
	idx, err := k.Index(ctx)
	if err != nil || !reflect.DeepEqual(idx, map[string][]string{"a": {"token"}, "b": {"password", "token"}, "c": {"token"}}) {
		t.Fatalf("Index = %v, %v", idx, err)
	}
	if v, ok, err := k.Get(ctx, "b", "password"); v != "p" || !ok || err != nil {
		t.Fatalf("Get = %q %v %v", v, ok, err)
	}
	if _, ok, _ := k.Get(ctx, "zz", "password"); ok {
		t.Fatal("missing secret")
	}
	if err := k.Set(ctx, "c", "token", ""); err != nil || fh.Secrets["profile.c.token"] != "" {
		t.Fatal("empty Set must delete")
	}
	fh.Calls = nil
	n, err := k.Forget(ctx, map[string]bool{"a": true}, true)
	if err != nil || n != 2 {
		t.Fatalf("Forget keep = %d, %v", n, err)
	}
	want := []string{
		`host.secrets.list {}`,
		`host.secrets.delete {"key":"profile.b.password"}`,
		`host.secrets.delete {"key":"profile.b.token"}`,
	}
	if !reflect.DeepEqual(fh.Calls, want) {
		t.Fatalf("calls = %q", fh.Calls)
	}
	if n, _ := k.Forget(ctx, map[string]bool{"a": true}, false); n != 1 || len(fh.Secrets) != 1 {
		t.Fatalf("Forget ids = %d, %v", n, fh.Secrets)
	}

	wrapped := host.CallerFunc(func(context.Context, string, any) (json.RawMessage, error) {
		return json.RawMessage(`{"keys":["profile.q.token"]}`), nil
	})
	if keys, err := profiles.ListKeys(host.WithCaller(ctx, wrapped)); err != nil || len(keys) != 1 {
		t.Fatalf("wrapped list = %v, %v", keys, err)
	}
	bad := host.CallerFunc(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`7`), nil })
	if _, err := profiles.ListKeys(host.WithCaller(ctx, bad)); err != profiles.ErrBadList {
		t.Fatalf("bad list: %v", err)
	}
}

func TestClassifyCodes(t *testing.T) {
	strict := []int{host.CodeCapabilityMissing}
	if profiles.ClassifyCodes(&host.Error{Code: -32003}, strict...) != profiles.Failed ||
		profiles.ClassifyCodes(&host.Error{Code: -32001}, strict...) != profiles.Denied ||
		profiles.ClassifyCodes(nil, strict...) != profiles.NotFailed ||
		profiles.ClassifyCodes(context.Canceled, strict...) != profiles.TimedOut {
		t.Fatal("ClassifyCodes")
	}
	if !profiles.Unreadable(&host.Error{Code: -32002}) || profiles.Unreadable(&host.Error{Code: -32001}) || profiles.Unreadable(nil) {
		t.Fatal("Unreadable")
	}
}

func TestConfirm(t *testing.T) {
	for _, c := range []struct {
		res                          string
		notFalseOrNull, notFalse, to bool
	}{
		{"true", true, true, true},
		{"false", false, false, false},
		{" false\n", false, false, false},
		{"null", false, true, false},
		{`"false"`, true, true, false},
		{"0", true, true, false},
		{`{"ok":true}`, true, true, true},
		{"", true, true, false},
	} {
		r := json.RawMessage(c.res)
		if profiles.ConfirmNotFalseOrNull(r) != c.notFalseOrNull || profiles.ConfirmNotFalse(r) != c.notFalse || profiles.ConfirmTrueOrObject(r) != c.to {
			t.Errorf("confirm(%q)", c.res)
		}
	}
	fh := rpctest.NewFakeHost()
	ctx := fh.Context(context.Background())
	answer := func(v string) context.Context {
		return host.WithCaller(ctx, host.CallerFunc(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(v), nil }))
	}
	var def profiles.Store
	if def.Save(answer("null"), []int{}) != profiles.ErrNotConfirmed || def.Save(answer(`"ok"`), []int{}) != nil {
		t.Fatal("default confirm")
	}
	lenient := profiles.Store{Confirm: profiles.ConfirmNotFalse}
	if lenient.Save(answer("null"), []int{}) != nil || lenient.Save(answer("false"), []int{}) != profiles.ErrNotConfirmed {
		t.Fatal("ConfirmNotFalse store")
	}
	k := profiles.Secrets{ConfirmSet: profiles.ConfirmNotFalse, ConfirmDelete: profiles.ConfirmNotFalseOrNull}
	if k.Set(answer("false"), "a", "f", "v") != profiles.ErrNotConfirmed || k.Set(answer("null"), "a", "f", "v") != nil ||
		k.Set(answer("null"), "a", "f", "") != profiles.ErrNotConfirmed || k.DeleteKey(answer("true"), "x") != nil {
		t.Fatal("secret confirm")
	}
	if (profiles.Secrets{}).Set(answer("false"), "a", "f", "v") != nil {
		t.Fatal("nil ConfirmSet ignores the result")
	}
}

func TestDecodeOptions(t *testing.T) {
	type want struct {
		n                int
		missing, damaged bool
	}
	check := func(s *profiles.Store, raw string, w want) {
		t.Helper()
		l := s.Decode(json.RawMessage(raw))
		if len(l.Entries) != w.n || l.Entries == nil || l.Missing != w.missing || l.Damaged != w.damaged {
			t.Errorf("%+v Decode(%s) = %d entries missing=%v damaged=%v", s.Field, raw, len(l.Entries), l.Missing, l.Damaged)
		}
	}
	def := &profiles.Store{}
	check(def, ``, want{0, true, false})
	check(def, " null ", want{0, true, false})
	check(def, `{"version":1,"profiles":[1,2]}`, want{2, false, false})
	check(def, `{"version":1}`, want{0, false, false})
	check(def, `{"version":1,"profiles":null}`, want{0, false, false})
	check(def, `{"version":2,"profiles":[1]}`, want{0, false, true})
	check(def, `{"version":"1","profiles":[1]}`, want{0, false, true})
	check(def, `{"version":1,"profiles":{}}`, want{0, false, true})
	check(def, `[1]`, want{0, false, true})
	check(def, `{nope`, want{0, false, true})
	check(def, `"{\"version\":1,\"profiles\":[1]}"`, want{1, false, false})
	// encoding/json member matching is kept: case folding, last duplicate wins.
	check(def, `{"VERSION":1,"Profiles":[1]}`, want{1, false, false})
	check(def, `{"version":1,"profiles":[1],"profiles":[1,2,3]}`, want{3, false, false})

	noUnwrap := &profiles.Store{NoUnwrap: true}
	check(noUnwrap, `"{\"version\":1,\"profiles\":[1]}"`, want{0, false, true})
	check(noUnwrap, `"null"`, want{0, false, true})
	strict := &profiles.Store{Strict: true}
	check(strict, `{"version":1,"profiles":[1],"profiles":[1]}`, want{0, false, true})
	check(strict, `{"version":1,"profiles":[{"a":1,"a":2}]}`, want{0, false, true})
	check(strict, `{"version":1,"profiles":[1]}`, want{1, false, false})
	emptyDamaged := &profiles.Store{EmptyDamaged: true}
	check(emptyDamaged, ``, want{0, false, true})
	check(emptyDamaged, " \n", want{0, false, true})
	check(emptyDamaged, `null`, want{0, true, false})
	envs := &profiles.Store{Field: "environments"}
	check(envs, `{"version":1,"environments":[1,2]}`, want{2, false, false})
	check(envs, `{"version":1,"profiles":[1,2]}`, want{0, false, false})

	if l := noUnwrap.Decode(json.RawMessage(` [1] `)); string(l.Raw) != `[1]` {
		t.Fatalf("Raw = %s", l.Raw)
	}
	if l := def.Decode(json.RawMessage(`"[1]"`)); string(l.Raw) != `[1]` {
		t.Fatalf("unwrapped Raw = %s", l.Raw)
	}
	fh := rpctest.NewFakeHost()
	ctx := fh.Context(context.Background())
	if l, err := def.Read(ctx); err != nil || !l.Missing {
		t.Fatalf("Read missing = %+v, %v", l, err)
	}
}

func TestEncodeOptions(t *testing.T) {
	fh := rpctest.NewFakeHost()
	ctx := fh.Context(context.Background())
	// wire records the params line as host.Client writes it (no HTML escaping).
	var wire string
	wireCtx := host.WithCaller(ctx, host.CallerFunc(func(_ context.Context, _ string, params any) (json.RawMessage, error) {
		b, err := jsonx.Marshal(params)
		wire = string(b)
		return json.RawMessage("true"), err
	}))
	esc := "a" + "\\" + "u0026" + "\\" + "u003cb" + "\\" + "u003e" // json.Marshal form of "a&<b>"
	list := []map[string]string{{"name": "a&<b>"}}
	cases := []struct {
		s      *profiles.Store
		stored string
	}{
		{&profiles.Store{}, `{"version":1,"profiles":[{"name":"a&<b>"}]}`},
		{&profiles.Store{EscapeHTML: true}, `{"version":1,"profiles":[{"name":"` + esc + `"}]}`},
		{&profiles.Store{ProfilesFirst: true}, `{"profiles":[{"name":"a&<b>"}],"version":1}`},
		{&profiles.Store{Field: "environments", Version: 3}, `{"version":3,"environments":[{"name":"a&<b>"}]}`},
	}
	for _, c := range cases {
		c.s.Key = "k"
		if err := c.s.Save(wireCtx, list); err != nil || wire != `{"key":"k","value":`+c.stored+`}` {
			t.Errorf("Save wrote %s, %v; want %s", wire, err, c.stored)
		}
		if m, _ := c.s.Marshal(list); string(m) != c.stored {
			t.Errorf("Marshal = %s", m)
		}
	}
	// The default wire form equals what Save sent before Marshal existed
	// (the Document struct encoded by the host client).
	old, _ := jsonx.Marshal(map[string]any{"key": "k", "value": profiles.Document{Version: 1, Profiles: list}})
	_ = (&profiles.Store{Key: "k"}).Save(wireCtx, list)
	if wire != string(old) {
		t.Fatalf("default wire %s vs %s", wire, old)
	}
	// ProfilesFirst matches what a Go map document encodes to.
	s := profiles.Store{ProfilesFirst: true, EscapeHTML: true}
	m, _ := s.Marshal(list)
	if b, _ := json.Marshal(map[string]any{"version": 1, "profiles": list}); string(b) != string(m) {
		t.Fatalf("map form %s vs %s", b, m)
	}
	// Encode is always the json.Marshal form (what size limits measure).
	if b, _ := (&profiles.Store{}).Encode(list); string(b) != `{"version":1,"profiles":[{"name":"`+esc+`"}]}` {
		t.Fatalf("Encode = %s", b)
	}
	capped := profiles.Store{Key: "capped", MaxBytes: 40}
	fh.Calls = nil
	if err := capped.Save(ctx, list); err != profiles.ErrTooLarge || len(fh.Calls) != 0 {
		t.Fatalf("MaxBytes: %v, calls %q", err, fh.Calls)
	}
	if err := capped.SaveRaw(ctx, []byte(`{"version":1,"profiles":[]}`)); err != nil || string(fh.Storage["capped"]) != `{"version":1,"profiles":[]}` {
		t.Fatalf("SaveRaw = %s, %v", fh.Storage["capped"], err)
	}
}

func TestSecretsOptions(t *testing.T) {
	fh := rpctest.NewFakeHost()
	ctx := fh.Context(context.Background())
	k := profiles.Secrets{
		Prefix:  "env.",
		IDValid: func(id string) bool { return strings.HasPrefix(id, "e-") },
		Fields:  []string{"auth.token", "auth.password"},
		Dynamic: []profiles.Pattern{{Prefix: "header.", Valid: func(n string) bool { return n == strings.ToLower(n) }, MaxBytes: 8}},
		Limits:  map[string]int{"auth.token": 16},
	}
	for key, ok := range map[string]bool{
		"env.e-1.auth.token": true, "env.e-1.header.x-api-key": true, "env.e-1.header.X": false,
		"env.x-1.auth.token": false, "env.e-1.other": false, "profile.e-1.auth.token": false,
	} {
		if _, _, got := k.Parse(key); got != ok {
			t.Errorf("Parse(%s) = %v", key, got)
		}
	}
	if n, ok := k.Limit("auth.token"); n != 16 || !ok {
		t.Fatal("Limit fixed")
	}
	if n, ok := k.Limit("header.a"); n != 8 || !ok {
		t.Fatal("Limit dynamic")
	}
	if n, ok := k.Limit("auth.password"); n != 0 || !ok {
		t.Fatal("Limit unlisted")
	}
	if _, ok := k.Limit("nope"); ok {
		t.Fatal("Limit unknown")
	}
	for _, key := range []string{"env.e-1.header.b", "env.e-1.auth.password", "env.e-1.header.a", "env.e-1.auth.token", "env.e-2.header.a"} {
		fh.Secrets[key] = "v"
	}
	idx, err := k.Index(ctx)
	if err != nil || !reflect.DeepEqual(idx["e-1"], []string{"auth.token", "auth.password", "header.a", "header.b"}) {
		t.Fatalf("Index = %v, %v", idx, err)
	}
	k.Sorted = true
	if idx, _ = k.Index(ctx); !reflect.DeepEqual(idx["e-1"], []string{"auth.password", "auth.token", "header.a", "header.b"}) {
		t.Fatalf("sorted Index = %v", idx)
	}
	if got := k.IndexKeys([]string{"env.e-3.auth.token", "env.e-3.auth.token"}); len(got["e-3"]) != 1 {
		t.Fatalf("duplicates = %v", got)
	}
	dup := k
	dup.KeepDuplicates = true
	if got := dup.IndexKeys([]string{"env.e-3.auth.token", "env.e-3.auth.password", "env.e-3.auth.token"}); !reflect.DeepEqual(got["e-3"], []string{"auth.password", "auth.token", "auth.token"}) {
		t.Fatalf("KeepDuplicates = %v", got)
	}

	// Sweep: predicate, order, best effort.
	fh.Calls = nil
	fh.Fail = func(method string, params json.RawMessage) error {
		if strings.Contains(string(params), "e-1.auth.token") {
			return &host.Error{Code: -32002}
		}
		return nil
	}
	n, err := k.Sweep(ctx, func(id, field string) bool { return strings.HasPrefix(field, "header.") || field == "auth.token" }, true)
	if n != 3 || !profiles.Unreadable(err) {
		t.Fatalf("best-effort Sweep = %d, %v", n, err)
	}
	want := []string{
		`host.secrets.list {}`,
		`host.secrets.delete {"key":"env.e-1.auth.token"}`,
		`host.secrets.delete {"key":"env.e-1.header.a"}`,
		`host.secrets.delete {"key":"env.e-1.header.b"}`,
		`host.secrets.delete {"key":"env.e-2.header.a"}`,
	}
	if !reflect.DeepEqual(fh.Calls, want) {
		t.Fatalf("calls = %q", fh.Calls)
	}
	if n, err := k.Sweep(ctx, func(string, string) bool { return true }, false); n != 1 || err == nil { // auth.password deleted, auth.token fails
		t.Fatalf("stopping Sweep = %d, %v", n, err)
	}
	fh.Fail = nil

	// StrictList, GetRaw.
	answer := func(v string) context.Context {
		return host.WithCaller(ctx, host.CallerFunc(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(v), nil }))
	}
	strictList := profiles.Secrets{StrictList: true}
	if _, err := strictList.List(answer(`{"keys":[]}`)); err != profiles.ErrBadList {
		t.Fatalf("strict wrapped: %v", err)
	}
	if keys, err := strictList.List(answer(`null`)); err != nil || keys != nil {
		t.Fatalf("strict null: %v %v", keys, err)
	}
	if keys, err := (profiles.Secrets{}).List(answer(`{"keys":["a"]}`)); err != nil || len(keys) != 1 {
		t.Fatalf("lenient wrapped: %v %v", keys, err)
	}
	for _, c := range []struct {
		res     string
		v       string
		present bool
		err     error
	}{{`"x"`, "x", true, nil}, {`""`, "", true, nil}, {`null`, "", false, nil}, {`7`, "", false, profiles.ErrBadValue}} {
		v, present, err := k.GetRaw(answer(c.res), "e-1", "auth.token")
		if v != c.v || present != c.present || err != c.err {
			t.Errorf("GetRaw(%s) = %q %v %v", c.res, v, present, err)
		}
		if v, ok, err := k.Get(answer(c.res), "e-1", "auth.token"); ok != (c.v != "") || v != c.v || err != nil {
			t.Errorf("Get(%s) = %q %v %v", c.res, v, ok, err)
		}
	}
	if !(profiles.Secrets{}).ValidID("p1") || (profiles.Secrets{}).ValidID("") {
		t.Fatal("ValidID")
	}
}
