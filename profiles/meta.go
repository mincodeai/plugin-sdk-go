package profiles

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/jsonx"
)

// Connection groups, environments and workbench preferences live in their own
// storage document beside the profile document (which is never touched):
//
//	{"version":1,"meta":{"<id>":{"group":"…","environment":"…"}},
//	 "prefs":{"collapsed":[…],"width":240,"recent":[…]}}
//
// "prefs" is omitted while it holds the defaults.

const (
	metaVersion       = 1
	metaGroupMaxRunes = 40
	prefsMaxCollapsed = 100
	prefsMaxRecent    = 5
	prefsMinWidth     = 180
	prefsMaxWidth     = 360
	prefsDefaultWidth = 240
)

// Meta classifies one saved connection. Both fields are optional; an empty
// Group means ungrouped.
type Meta struct {
	Group       string `json:"group,omitempty"`
	Environment string `json:"environment,omitempty"`
}

func (m Meta) empty() bool { return m.Group == "" && m.Environment == "" }

// Prefs are the connection sidebar preferences: collapsed group names ("" is
// the ungrouped section), sidebar width in pixels and recently used ids
// (most recent first).
type Prefs struct {
	Collapsed []string `json:"collapsed"`
	Width     int      `json:"width"`
	Recent    []string `json:"recent"`
}

func defaultPrefs() Prefs {
	return Prefs{Collapsed: []string{}, Width: prefsDefaultWidth, Recent: []string{}}
}

func (p Prefs) isDefault() bool {
	return len(p.Collapsed) == 0 && p.Width == prefsDefaultWidth && len(p.Recent) == 0
}

// MetaError is a validation failure. Field names the offending input
// ("group", "environment", "id", "from", "collapsed", "width", "payload");
// Error returns the user-facing (Chinese) Reason.
type MetaError struct {
	Field  string
	Reason string
}

func (e *MetaError) Error() string {
	if e.Reason == "" {
		return e.Field + " 无效"
	}
	return e.Reason
}

var environments = []string{"dev", "test", "staging", "prod"}

// normalizeGroup trims name and checks the group-name rules (empty allowed).
func normalizeGroup(name, field string) (string, error) {
	name = strings.TrimSpace(name)
	if !utf8.ValidString(name) {
		return "", &MetaError{Field: field, Reason: "分组名包含无效字符"}
	}
	if utf8.RuneCountInString(name) > metaGroupMaxRunes {
		return "", &MetaError{Field: field, Reason: fmt.Sprintf("分组名最多 %d 个字符", metaGroupMaxRunes)}
	}
	if strings.IndexFunc(name, func(r rune) bool { return unicode.IsControl(r) || r == ' ' || r == ' ' }) >= 0 {
		return "", &MetaError{Field: field, Reason: "分组名不能包含换行或控制字符"}
	}
	return name, nil
}

// NormalizeMeta trims both fields and validates them: Group 0–40 runes
// without newlines or control characters, Environment one of dev, test,
// staging, prod or empty. Failures are *MetaError.
func NormalizeMeta(m Meta) (Meta, error) {
	group, err := normalizeGroup(m.Group, "group")
	if err != nil {
		return Meta{}, err
	}
	env := strings.TrimSpace(m.Environment)
	if env != "" && !slices.Contains(environments, env) {
		return Meta{}, &MetaError{Field: "environment", Reason: "环境只能是 dev、test、staging 或 prod"}
	}
	return Meta{Group: group, Environment: env}, nil
}

// MetaStore reads and writes the group/environment document. The zero value
// is usable; methods take the host storage caller from ctx like Store and
// serialise their read-modify-write cycles. Host errors are returned raw
// (see Classify); an unconfirmed write is ErrNotConfirmed.
type MetaStore struct {
	// Key is the storage key (default "profileMeta").
	Key string
	// MaxEntries limits how many connections may carry metadata (default 1000).
	MaxEntries int
	mu         sync.Mutex
}

func (s *MetaStore) key() string {
	if s.Key == "" {
		return "profileMeta"
	}
	return s.Key
}

func (s *MetaStore) maxEntries() int {
	if s.MaxEntries <= 0 {
		return 1000
	}
	return s.MaxEntries
}

type metaDoc struct {
	Meta  map[string]Meta
	Prefs Prefs
}

// read loads the document; a missing, corrupt or foreign-version value reads
// as empty. Invalid entries are dropped and prefs are sanitised.
func (s *MetaStore) read(ctx context.Context) (metaDoc, error) {
	doc := metaDoc{Meta: map[string]Meta{}, Prefs: defaultPrefs()}
	h := host.FromContext(ctx)
	if h == nil {
		return doc, host.ErrUnavailable
	}
	raw, err := h.Call(ctx, "host.storage.get", map[string]any{"key": s.key()})
	if err != nil {
		return doc, err
	}
	raw = bytes.TrimSpace(raw)
	var text string
	if json.Unmarshal(raw, &text) == nil {
		raw = []byte(strings.TrimSpace(text))
	}
	var stored struct {
		Version int                        `json:"version"`
		Meta    map[string]json.RawMessage `json:"meta"`
		Prefs   json.RawMessage            `json:"prefs"`
	}
	if json.Unmarshal(raw, &stored) != nil || stored.Version != metaVersion {
		return doc, nil
	}
	for id, entry := range stored.Meta {
		var m Meta
		if id == "" || json.Unmarshal(entry, &m) != nil {
			continue
		}
		if m, err := NormalizeMeta(m); err == nil && !m.empty() {
			doc.Meta[id] = m
		}
	}
	var p struct {
		Collapsed []string `json:"collapsed"`
		Width     int      `json:"width"`
		Recent    []string `json:"recent"`
	}
	if len(stored.Prefs) > 0 && json.Unmarshal(stored.Prefs, &p) == nil {
		if p.Width >= prefsMinWidth && p.Width <= prefsMaxWidth {
			doc.Prefs.Width = p.Width
		}
		for _, name := range p.Collapsed {
			if name, err := normalizeGroup(name, "collapsed"); err == nil && !slices.Contains(doc.Prefs.Collapsed, name) &&
				len(doc.Prefs.Collapsed) < prefsMaxCollapsed {
				doc.Prefs.Collapsed = append(doc.Prefs.Collapsed, name)
			}
		}
		for _, id := range p.Recent {
			if id != "" && !slices.Contains(doc.Prefs.Recent, id) && len(doc.Prefs.Recent) < prefsMaxRecent {
				doc.Prefs.Recent = append(doc.Prefs.Recent, id)
			}
		}
	}
	return doc, nil
}

func (s *MetaStore) write(ctx context.Context, doc metaDoc) error {
	out := struct {
		Version int             `json:"version"`
		Meta    map[string]Meta `json:"meta"`
		Prefs   *Prefs          `json:"prefs,omitempty"`
	}{Version: metaVersion, Meta: doc.Meta}
	if !doc.Prefs.isDefault() {
		out.Prefs = &doc.Prefs
	}
	b, err := jsonx.Marshal(out)
	if err != nil {
		return err
	}
	h := host.FromContext(ctx)
	if h == nil {
		return host.ErrUnavailable
	}
	res, err := h.Call(ctx, "host.storage.set", map[string]any{"key": s.key(), "value": json.RawMessage(b)})
	if err != nil {
		return err
	}
	if !ConfirmNotFalseOrNull(res) {
		return ErrNotConfirmed
	}
	return nil
}

// Load returns every stored entry (never nil). A missing, corrupt or
// foreign-version document yields an empty map.
func (s *MetaStore) Load(ctx context.Context) (map[string]Meta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read(ctx)
	if err != nil {
		return nil, err
	}
	return doc.Meta, nil
}

// Set normalises m and stores it for id; an m with both fields empty removes
// the entry. It returns every stored entry.
func (s *MetaStore) Set(ctx context.Context, id string, m Meta) (map[string]Meta, error) {
	m, err := NormalizeMeta(m)
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, &MetaError{Field: "id", Reason: "连接不存在"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read(ctx)
	if err != nil {
		return nil, err
	}
	old, had := doc.Meta[id]
	switch {
	case m.empty() && !had, had && old == m:
		return doc.Meta, nil
	case m.empty():
		delete(doc.Meta, id)
	case !had && len(doc.Meta) >= s.maxEntries():
		return nil, &MetaError{Field: "id", Reason: fmt.Sprintf("最多为 %d 个连接设置分组或环境", s.maxEntries())}
	default:
		doc.Meta[id] = m
	}
	if err := s.write(ctx, doc); err != nil {
		return nil, err
	}
	return doc.Meta, nil
}

// RenameGroup moves every entry (and the collapsed-group preference) from
// group from to group to in one write. An empty to deletes the group: the
// entries lose their Group and entries left empty are removed. Names are
// compared case-sensitively after trimming. It returns every stored entry.
func (s *MetaStore) RenameGroup(ctx context.Context, from, to string) (map[string]Meta, error) {
	from = strings.TrimSpace(from)
	if from == "" {
		return nil, &MetaError{Field: "from", Reason: "原分组名不能为空"}
	}
	to, err := normalizeGroup(to, "group")
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read(ctx)
	if err != nil {
		return nil, err
	}
	if from == to {
		return doc.Meta, nil
	}
	changed := false
	for id, m := range doc.Meta {
		if m.Group != from {
			continue
		}
		changed = true
		m.Group = to
		if m.empty() {
			delete(doc.Meta, id)
		} else {
			doc.Meta[id] = m
		}
	}
	if i := slices.Index(doc.Prefs.Collapsed, from); i >= 0 {
		changed = true
		if to == "" || slices.Contains(doc.Prefs.Collapsed, to) {
			doc.Prefs.Collapsed = slices.Delete(doc.Prefs.Collapsed, i, i+1)
		} else {
			doc.Prefs.Collapsed[i] = to
		}
	}
	if !changed {
		return doc.Meta, nil
	}
	if err := s.write(ctx, doc); err != nil {
		return nil, err
	}
	return doc.Meta, nil
}

// Delete removes the entries of ids and drops them from the recent list
// (call it when profiles are deleted). Nothing is written when none of the
// ids is stored.
func (s *MetaStore) Delete(ctx context.Context, ids ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read(ctx)
	if err != nil {
		return err
	}
	changed := false
	for _, id := range ids {
		if _, ok := doc.Meta[id]; ok {
			delete(doc.Meta, id)
			changed = true
		}
	}
	recent := slices.DeleteFunc(slices.Clone(doc.Prefs.Recent), func(id string) bool { return slices.Contains(ids, id) })
	if len(recent) != len(doc.Prefs.Recent) {
		doc.Prefs.Recent = recent
		changed = true
	}
	if !changed {
		return nil
	}
	return s.write(ctx, doc)
}

func decodeMetaPayload(payload json.RawMessage, v any) error {
	if t := bytes.TrimSpace(payload); string(t) == "null" {
		payload = nil
	}
	if err := jsonx.DecodeObject(payload, v, jsonx.DecodeOptions{Strict: true, DisallowUnknown: true, EmptyAsObject: true}); err != nil {
		return &MetaError{Field: "payload", Reason: "请求参数无效"}
	}
	return nil
}

func knownSet(ctx context.Context, known func(context.Context) ([]string, error)) (map[string]bool, error) {
	ids, err := known(ctx)
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set, nil
}

func filterMeta(meta map[string]Meta, known map[string]bool) map[string]Meta {
	out := make(map[string]Meta, len(meta))
	for id, m := range meta {
		if known[id] {
			out[id] = m
		}
	}
	return out
}

func filterPrefs(p Prefs, known map[string]bool) Prefs {
	p.Collapsed = slices.Clone(p.Collapsed)
	p.Recent = slices.DeleteFunc(slices.Clone(p.Recent), func(id string) bool { return !known[id] })
	return p
}

// Handle serves the plugin actions for groups and preferences; other actions
// return handled=false without touching the host. known lists the ids of
// the saved profiles: responses only include those ids and setProfileMeta
// rejects any other.
//
//	profileMeta        {}                                → {"meta":{…},"prefs":{…}}
//	setProfileMeta     {"id","group","environment"}      → {"meta":{…}}
//	renameProfileGroup {"from","to"}                     → {"meta":{…}}
//	setConnectionPrefs {"collapsed","width","recent"}    → {"prefs":{…}}
//
// setConnectionPrefs keeps omitted fields. Payloads are decoded strictly
// (unknown fields are errors); validation failures are *MetaError.
func (s *MetaStore) Handle(ctx context.Context, action string, payload json.RawMessage, known func(context.Context) ([]string, error)) (any, bool, error) {
	var (
		res any
		err error
	)
	switch action {
	case "profileMeta":
		res, err = s.handleGet(ctx, payload, known)
	case "setProfileMeta":
		res, err = s.handleSet(ctx, payload, known)
	case "renameProfileGroup":
		res, err = s.handleRename(ctx, payload, known)
	case "setConnectionPrefs":
		res, err = s.handlePrefs(ctx, payload, known)
	default:
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	return res, true, nil
}

func (s *MetaStore) handleGet(ctx context.Context, payload json.RawMessage, known func(context.Context) ([]string, error)) (any, error) {
	if err := decodeMetaPayload(payload, &struct{}{}); err != nil {
		return nil, err
	}
	ids, err := knownSet(ctx, known)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	doc, err := s.read(ctx)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return map[string]any{"meta": filterMeta(doc.Meta, ids), "prefs": filterPrefs(doc.Prefs, ids)}, nil
}

func (s *MetaStore) handleSet(ctx context.Context, payload json.RawMessage, known func(context.Context) ([]string, error)) (any, error) {
	var p struct {
		ID          string `json:"id"`
		Group       string `json:"group"`
		Environment string `json:"environment"`
	}
	if err := decodeMetaPayload(payload, &p); err != nil {
		return nil, err
	}
	ids, err := knownSet(ctx, known)
	if err != nil {
		return nil, err
	}
	if !ids[p.ID] {
		return nil, &MetaError{Field: "id", Reason: "连接不存在"}
	}
	meta, err := s.Set(ctx, p.ID, Meta{Group: p.Group, Environment: p.Environment})
	if err != nil {
		return nil, err
	}
	return map[string]any{"meta": filterMeta(meta, ids)}, nil
}

func (s *MetaStore) handleRename(ctx context.Context, payload json.RawMessage, known func(context.Context) ([]string, error)) (any, error) {
	var p struct {
		From string `json:"from"`
		To   string `json:"to"`
	}
	if err := decodeMetaPayload(payload, &p); err != nil {
		return nil, err
	}
	ids, err := knownSet(ctx, known)
	if err != nil {
		return nil, err
	}
	meta, err := s.RenameGroup(ctx, p.From, p.To)
	if err != nil {
		return nil, err
	}
	return map[string]any{"meta": filterMeta(meta, ids)}, nil
}

func (s *MetaStore) handlePrefs(ctx context.Context, payload json.RawMessage, known func(context.Context) ([]string, error)) (any, error) {
	var p struct {
		Collapsed *[]string `json:"collapsed"`
		Width     *int      `json:"width"`
		Recent    *[]string `json:"recent"`
	}
	if err := decodeMetaPayload(payload, &p); err != nil {
		return nil, err
	}
	var collapsed []string
	if p.Collapsed != nil {
		if len(*p.Collapsed) > prefsMaxCollapsed {
			return nil, &MetaError{Field: "collapsed", Reason: fmt.Sprintf("折叠的分组最多 %d 个", prefsMaxCollapsed)}
		}
		collapsed = []string{}
		for _, name := range *p.Collapsed {
			name, err := normalizeGroup(name, "collapsed")
			if err != nil {
				return nil, err
			}
			if !slices.Contains(collapsed, name) {
				collapsed = append(collapsed, name)
			}
		}
	}
	if p.Width != nil && (*p.Width < prefsMinWidth || *p.Width > prefsMaxWidth) {
		return nil, &MetaError{Field: "width", Reason: fmt.Sprintf("左栏宽度必须在 %d 到 %d 之间", prefsMinWidth, prefsMaxWidth)}
	}
	ids, err := knownSet(ctx, known)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read(ctx)
	if err != nil {
		return nil, err
	}
	if p.Collapsed != nil {
		doc.Prefs.Collapsed = collapsed
	}
	if p.Width != nil {
		doc.Prefs.Width = *p.Width
	}
	if p.Recent != nil {
		recent := []string{}
		for _, id := range *p.Recent {
			if ids[id] && !slices.Contains(recent, id) && len(recent) < prefsMaxRecent {
				recent = append(recent, id)
			}
		}
		doc.Prefs.Recent = recent
	}
	if err := s.write(ctx, doc); err != nil {
		return nil, err
	}
	return map[string]any{"prefs": filterPrefs(doc.Prefs, ids)}, nil
}
