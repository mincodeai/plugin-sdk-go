// Package profiles holds the connection-profile plumbing shared by the
// official plugins: a versioned profile document in host storage
// ({"version":1,"profiles":[...]}) and per-field credentials in the host's
// encrypted secret store under "profile.<id>.<field>" keys. Profile shape,
// validation and user-facing error wording stay in the plugin; this package
// only moves bytes and classifies host failures.
//
// Every option defaults to the original behaviour, so the zero Store and a
// Secrets with only IDPattern/Fields behave exactly as before. The options
// exist so plugins with older, slightly different storage rules can use the
// same code without changing a stored byte.
package profiles

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/jsonx"
)

// Failure classifies an error returned by a host storage or secrets call so a
// plugin can map it to its own message.
type Failure int

const (
	// NotFailed: err was nil.
	NotFailed Failure = iota
	// Denied: the capability is not granted (-32001) or reverse requests are
	// unsupported by the host (-32003).
	Denied
	// Failed: any other host error (e.g. -32002 disk full, undecryptable secret).
	Failed
	// TimedOut: the call's context was cancelled or its deadline passed.
	TimedOut
	// Unavailable: the transport stopped or the host never answered.
	Unavailable
)

// Classify maps a host call error to a Failure (-32001 and -32003 are Denied).
func Classify(err error) Failure {
	return ClassifyCodes(err, host.CodeCapabilityMissing, host.CodeHostRequestFailed)
}

// ClassifyCodes is Classify with an explicit set of host error codes that
// count as Denied; every other *host.Error is Failed. Plugins that always
// reported -32003 as a failed request use ClassifyCodes(err, host.CodeCapabilityMissing).
func ClassifyCodes(err error, denied ...int) Failure {
	switch {
	case err == nil:
		return NotFailed
	case host.IsCode(err, denied...):
		return Denied
	case errors.As(err, new(*host.Error)):
		return Failed
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return TimedOut
	}
	return Unavailable
}

// Unreadable reports whether err is the host's -32002 storage failure, which
// host.secrets.get uses for a value that cannot be decrypted on this device.
func Unreadable(err error) bool { return host.IsCode(err, host.CodeStorageFailure) }

// ConfirmFunc decides whether a host write result (host.storage.set,
// host.secrets.set/delete) confirms the write.
type ConfirmFunc func(result json.RawMessage) bool

// ConfirmNotFalseOrNull rejects results that decode as a JSON boolean false,
// which includes null. It is Store's default rule.
func ConfirmNotFalseOrNull(result json.RawMessage) bool {
	var ok bool
	return json.Unmarshal(result, &ok) != nil || ok
}

// ConfirmNotFalse rejects only the literal false (surrounding whitespace
// ignored); null and any other value confirm.
func ConfirmNotFalse(result json.RawMessage) bool {
	return !bytes.Equal(bytes.TrimSpace(result), []byte("false"))
}

// ConfirmTrueOrObject accepts only true or a JSON object result.
func ConfirmTrueOrObject(result json.RawMessage) bool {
	t := bytes.TrimSpace(result)
	return bytes.Equal(t, []byte("true")) || bytes.HasPrefix(t, []byte("{"))
}

// ErrNotConfirmed is returned when the host answered a write with a result
// the configured ConfirmFunc rejects (by default: storage set answered false).
var ErrNotConfirmed = errors.New("profiles: host storage did not confirm the write")

// ErrTooLarge is returned by Store.Save when the document exceeds MaxBytes.
// Nothing is sent to the host.
var ErrTooLarge = errors.New("profiles: document exceeds the size limit")

// Store reads and writes the profile document. Lock/Unlock serialise
// read-modify-write cycles (and keep reverse requests in a stable order).
type Store struct {
	// Key is the storage key (default "profiles").
	Key string
	// Version is the document version (default 1). Documents with another
	// version load as empty.
	Version int
	// Field is the document member holding the entries (default "profiles";
	// e.g. "environments").
	Field string
	// NoUnwrap keeps a stored JSON string as is (it then reads as Damaged)
	// instead of decoding the document inside it.
	NoUnwrap bool
	// EmptyDamaged treats an empty or whitespace-only stored value as Damaged
	// (only null is Missing).
	EmptyDamaged bool
	// Strict rejects (as Damaged) a document that fails jsonx.Strict:
	// duplicate member names or invalid escapes anywhere.
	Strict bool
	// ProfilesFirst writes {"<Field>":[...],"version":N} (the member order a
	// Go map produces) instead of {"version":N,"<Field>":[...]}.
	ProfilesFirst bool
	// EscapeHTML writes <, > and & inside the document as < etc., like
	// json.Marshal, instead of verbatim.
	EscapeHTML bool
	// MaxBytes, when > 0, makes Save fail with ErrTooLarge (before any host
	// call) if the Encode form of the document is longer.
	MaxBytes int
	// Confirm judges the host.storage.set result (default ConfirmNotFalseOrNull).
	Confirm ConfirmFunc
	mu      sync.Mutex
}

// Lock acquires the store mutex.
func (s *Store) Lock() { s.mu.Lock() }

// Unlock releases the store mutex.
func (s *Store) Unlock() { s.mu.Unlock() }

func (s *Store) key() string {
	if s.Key == "" {
		return "profiles"
	}
	return s.Key
}

func (s *Store) version() int {
	if s.Version == 0 {
		return 1
	}
	return s.Version
}

func (s *Store) field() string {
	if s.Field == "" {
		return "profiles"
	}
	return s.Field
}

// Loaded is a decoded stored document.
type Loaded struct {
	// Entries are the raw entries (never nil; empty when Missing or Damaged).
	Entries []json.RawMessage
	// Raw is the stored value (trimmed, after string unwrapping unless
	// NoUnwrap); nil when Missing. Plugins migrating a legacy shape inspect it.
	Raw json.RawMessage
	// Missing: the host returned nothing, whitespace or null.
	Missing bool
	// Damaged: a value is stored but is not a readable document of this
	// version (corrupt JSON, wrong shape, foreign version, Strict failure).
	// A readable document without the entries member is not Damaged.
	Damaged bool
}

// Read fetches and decodes the document (host storage caller from ctx). The
// error is the raw host error; see Classify.
func (s *Store) Read(ctx context.Context) (Loaded, error) {
	h := host.FromContext(ctx)
	if h == nil {
		return Loaded{}, host.ErrUnavailable
	}
	raw, err := h.Call(ctx, "host.storage.get", map[string]any{"key": s.key()})
	if err != nil {
		return Loaded{}, err
	}
	return s.Decode(raw), nil
}

// Load returns the raw profile entries. A missing, corrupt or foreign-version
// document yields an empty list; a stored JSON string is unwrapped first
// (some hosts return values as text) unless NoUnwrap.
func (s *Store) Load(ctx context.Context) ([]json.RawMessage, error) {
	l, err := s.Read(ctx)
	if err != nil {
		return nil, err
	}
	return l.Entries, nil
}

// Decode decodes a stored value with the store's options.
func (s *Store) Decode(raw json.RawMessage) Loaded {
	out := Loaded{Entries: []json.RawMessage{}}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 && s.EmptyDamaged {
		out.Damaged = true
		return out
	}
	if len(raw) == 0 || string(raw) == "null" {
		out.Missing = true
		return out
	}
	if !s.NoUnwrap {
		var text string
		if json.Unmarshal(raw, &text) == nil {
			raw = bytes.TrimSpace([]byte(text))
		}
	}
	out.Raw = raw
	// Decode into struct{Version int `json:"version"`; Entries []json.RawMessage
	// `json:"<Field>"`} so member matching (case folding, last duplicate wins,
	// null keeps the zero value) is exactly encoding/json's, as in the plugins
	// that decoded a fixed struct.
	doc := reflect.New(s.docType())
	if (s.Strict && !jsonx.Strict(raw)) || json.Unmarshal(raw, doc.Interface()) != nil ||
		doc.Elem().Field(0).Int() != int64(s.version()) {
		out.Damaged = true
		return out
	}
	if entries := doc.Elem().Field(1).Interface().([]json.RawMessage); entries != nil {
		out.Entries = entries
	}
	return out
}

func (s *Store) docType() reflect.Type {
	return reflect.StructOf([]reflect.StructField{
		{Name: "Version", Type: reflect.TypeOf(0), Tag: `json:"version"`},
		{Name: "Entries", Type: reflect.TypeOf([]json.RawMessage(nil)), Tag: reflect.StructTag(`json:"` + s.field() + `"`)},
	})
}

// DecodeDocument extracts the profiles array of a stored document (default
// Store options).
func DecodeDocument(raw json.RawMessage, version int) []json.RawMessage {
	return (&Store{Version: version}).Decode(raw).Entries
}

// Document is the stored form {"version":N,"profiles":[...]}.
type Document struct {
	Version  int `json:"version"`
	Profiles any `json:"profiles"`
}

// render writes the document with the given escaping and the store's member
// order and names. Entries are marshalled exactly like a struct field.
func (s *Store) render(profiles any, escapeHTML bool) ([]byte, error) {
	list, err := jsonx.MarshalStyle(profiles, escapeHTML)
	if err != nil {
		return nil, err
	}
	name, _ := json.Marshal(s.field())
	version, _ := json.Marshal(s.version())
	var b bytes.Buffer
	b.WriteByte('{')
	if s.ProfilesFirst {
		b.Write(name)
		b.WriteByte(':')
		b.Write(list)
		b.WriteString(`,"version":`)
		b.Write(version)
	} else {
		b.WriteString(`"version":`)
		b.Write(version)
		b.WriteByte(',')
		b.Write(name)
		b.WriteByte(':')
		b.Write(list)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

// Encode renders the document in json.Marshal form (HTML-escaped), the size
// every plugin checks its limit against.
func (s *Store) Encode(profiles any) ([]byte, error) { return s.render(profiles, true) }

// Marshal renders the document exactly as Save writes it.
func (s *Store) Marshal(profiles any) ([]byte, error) { return s.render(profiles, s.EscapeHTML) }

// Save writes profiles (any JSON-marshalable slice) as the document.
func (s *Store) Save(ctx context.Context, profiles any) error {
	if s.MaxBytes > 0 {
		b, err := s.Encode(profiles)
		if err != nil {
			return err
		}
		if len(b) > s.MaxBytes {
			return ErrTooLarge
		}
	}
	doc, err := s.Marshal(profiles)
	if err != nil {
		return err
	}
	return s.SaveRaw(ctx, doc)
}

// SaveRaw writes an already rendered document (e.g. Marshal output after a
// plugin-specific size check) verbatim.
func (s *Store) SaveRaw(ctx context.Context, doc []byte) error {
	h := host.FromContext(ctx)
	if h == nil {
		return host.ErrUnavailable
	}
	res, err := h.Call(ctx, "host.storage.set", map[string]any{"key": s.key(), "value": json.RawMessage(doc)})
	if err != nil {
		return err
	}
	confirm := s.Confirm
	if confirm == nil {
		confirm = ConfirmNotFalseOrNull
	}
	if !confirm(res) {
		return ErrNotConfirmed
	}
	return nil
}

// Pattern allows a family of dynamic secret fields "<Prefix><name>" such as
// "header.authorization".
type Pattern struct {
	// Prefix of the field ("header."); an empty prefix matches every field.
	Prefix string
	// Valid, when set, must accept the part after Prefix.
	Valid func(name string) bool
	// MaxBytes is the value limit Limit reports for matching fields.
	MaxBytes int
}

func (p Pattern) match(field string) bool {
	name, ok := strings.CutPrefix(field, p.Prefix)
	return ok && (p.Valid == nil || p.Valid(name))
}

// Secrets names remembered credential fields "<Prefix><id>.<field>".
type Secrets struct {
	// Prefix defaults to "profile.".
	Prefix string
	// IDPattern validates profile ids (nil: any id without '.').
	IDPattern *regexp.Regexp
	// IDValid, when set, validates profile ids as well (for ids checked by
	// code rather than a regexp).
	IDValid func(id string) bool
	// Fields is the allow-list in display order; keys naming other fields
	// are ignored unless a Dynamic pattern matches them.
	Fields []string
	// Dynamic allows further fields by pattern (checked after Fields). Index
	// lists them after the fixed fields, in byte order.
	Dynamic []Pattern
	// Limits holds per-field value limits for Limit.
	Limits map[string]int
	// Sorted orders every id's fields by byte order instead of allow-list order.
	Sorted bool
	// StrictList accepts only a JSON array (or null) from host.secrets.list.
	StrictList bool
	// KeepDuplicates keeps a field listed twice by host.secrets.list twice in
	// Index (and so deletes it twice in Sweep) instead of once.
	KeepDuplicates bool
	// ConfirmSet / ConfirmDelete, when set, judge the host.secrets.set /
	// host.secrets.delete result; a rejected result returns ErrNotConfirmed.
	// nil ignores the result.
	ConfirmSet    ConfirmFunc
	ConfirmDelete ConfirmFunc
}

func (k Secrets) prefix() string {
	if k.Prefix == "" {
		return "profile."
	}
	return k.Prefix
}

// Key returns the secret key of one profile field.
func (k Secrets) Key(id, field string) string { return k.prefix() + id + "." + field }

// Allowed reports whether field is in the allow-list or matches a Dynamic pattern.
func (k Secrets) Allowed(field string) bool {
	for _, f := range k.Fields {
		if f == field {
			return true
		}
	}
	for _, p := range k.Dynamic {
		if p.match(field) {
			return true
		}
	}
	return false
}

// Limit returns the value limit of an allowed field: Limits[field], else the
// first matching Dynamic pattern's MaxBytes. ok is false for other fields.
func (k Secrets) Limit(field string) (limit int, ok bool) {
	if n, ok := k.Limits[field]; ok {
		return n, true
	}
	for _, f := range k.Fields {
		if f == field {
			return 0, true
		}
	}
	for _, p := range k.Dynamic {
		if p.match(field) {
			return p.MaxBytes, true
		}
	}
	return 0, false
}

// ValidID reports whether id passes IDPattern and IDValid (and has no '.').
func (k Secrets) ValidID(id string) bool {
	return id != "" && !strings.Contains(id, ".") &&
		(k.IDPattern == nil || k.IDPattern.MatchString(id)) && (k.IDValid == nil || k.IDValid(id))
}

// Parse splits a key into profile id and field; ok is false for foreign,
// malformed or non-allowed keys.
func (k Secrets) Parse(key string) (id, field string, ok bool) {
	rest, found := strings.CutPrefix(key, k.prefix())
	if !found {
		return "", "", false
	}
	id, field, found = strings.Cut(rest, ".")
	if !found || !k.ValidID(id) || !k.Allowed(field) {
		return "", "", false
	}
	return id, field, true
}

// Order sorts fields in place: allow-list order, then dynamic fields in
// byte order (or everything in byte order when Sorted).
func (k Secrets) Order(fields []string) {
	rank := func(f string) int {
		if !k.Sorted {
			for i, s := range k.Fields {
				if s == f {
					return i
				}
			}
		}
		return len(k.Fields)
	}
	sort.SliceStable(fields, func(i, j int) bool {
		ri, rj := rank(fields[i]), rank(fields[j])
		if ri != rj {
			return ri < rj
		}
		return fields[i] < fields[j]
	})
}

// ErrBadList is returned when host.secrets.list answers neither an array of
// keys nor {"keys":[...]}.
var ErrBadList = errors.New("profiles: unexpected host.secrets.list result")

// ErrBadValue is returned by GetRaw when host.secrets.get answers neither a
// string nor null.
var ErrBadValue = errors.New("profiles: unexpected host.secrets.get result")

// ListKeys calls host.secrets.list and returns every key (values are never
// read). Both [..] and {"keys":[..]} results are accepted.
func ListKeys(ctx context.Context) ([]string, error) { return listKeys(ctx, false) }

// List is ListKeys honouring StrictList.
func (k Secrets) List(ctx context.Context) ([]string, error) { return listKeys(ctx, k.StrictList) }

func listKeys(ctx context.Context, strict bool) ([]string, error) {
	h := host.FromContext(ctx)
	if h == nil {
		return nil, host.ErrUnavailable
	}
	raw, err := h.Call(ctx, "host.secrets.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var keys []string
	if json.Unmarshal(raw, &keys) == nil {
		return keys, nil
	}
	var wrapped struct {
		Keys []string `json:"keys"`
	}
	if strict || json.Unmarshal(raw, &wrapped) != nil {
		return nil, ErrBadList
	}
	return wrapped.Keys, nil
}

// Index maps profile id to its remembered fields (see Order).
func (k Secrets) Index(ctx context.Context) (map[string][]string, error) {
	keys, err := k.List(ctx)
	if err != nil {
		return nil, err
	}
	return k.IndexKeys(keys), nil
}

// IndexKeys is Index over an already listed key set (duplicates ignored
// unless KeepDuplicates).
func (k Secrets) IndexKeys(keys []string) map[string][]string {
	have := map[string]map[string]bool{}
	out := map[string][]string{}
	for _, key := range keys {
		if id, field, ok := k.Parse(key); ok {
			if have[id] == nil {
				have[id] = map[string]bool{}
			}
			if k.KeepDuplicates || !have[id][field] {
				have[id][field] = true
				out[id] = append(out[id], field)
			}
		}
	}
	for _, fields := range out {
		k.Order(fields)
	}
	return out
}

// GetRaw reads one field: present is false when the host returned null; a
// result that is neither a string nor null is ErrBadValue. Empty strings are
// returned as present.
func (k Secrets) GetRaw(ctx context.Context, id, field string) (value string, present bool, err error) {
	h := host.FromContext(ctx)
	if h == nil {
		return "", false, host.ErrUnavailable
	}
	raw, err := h.Call(ctx, "host.secrets.get", map[string]any{"key": k.Key(id, field)})
	if err != nil {
		return "", false, err
	}
	var v *string
	if json.Unmarshal(raw, &v) != nil {
		return "", false, ErrBadValue
	}
	if v == nil {
		return "", false, nil
	}
	return *v, true, nil
}

// Get reads one field. ok is false when the host returned null, an empty
// string or a non-string (removed meanwhile).
func (k Secrets) Get(ctx context.Context, id, field string) (value string, ok bool, err error) {
	value, ok, err = k.GetRaw(ctx, id, field)
	if errors.Is(err, ErrBadValue) {
		return "", false, nil
	}
	if err != nil || value == "" {
		return "", false, err
	}
	return value, ok, nil
}

// Set stores one field; an empty value deletes it instead.
func (k Secrets) Set(ctx context.Context, id, field, value string) error {
	if value == "" {
		return k.Delete(ctx, id, field)
	}
	return k.call(ctx, "host.secrets.set", map[string]any{"key": k.Key(id, field), "value": value}, k.ConfirmSet)
}

// Delete removes one field.
func (k Secrets) Delete(ctx context.Context, id, field string) error {
	return k.DeleteKey(ctx, k.Key(id, field))
}

// DeleteKey removes one secret by its full key (for cleanups of keys Parse
// rejects).
func (k Secrets) DeleteKey(ctx context.Context, key string) error {
	return k.call(ctx, "host.secrets.delete", map[string]any{"key": key}, k.ConfirmDelete)
}

func (k Secrets) call(ctx context.Context, method string, params map[string]any, confirm ConfirmFunc) error {
	h := host.FromContext(ctx)
	if h == nil {
		return host.ErrUnavailable
	}
	res, err := h.Call(ctx, method, params)
	if err == nil && confirm != nil && !confirm(res) {
		return ErrNotConfirmed
	}
	return err
}

// Forget deletes the remembered fields of the listed ids (keep=false) or of
// every id not listed (keep=true, garbage collection after an import or
// delete) and returns how many keys were deleted. Deletion order is sorted
// by id, then field order, so reverse requests are deterministic. It stops
// at the first error.
func (k Secrets) Forget(ctx context.Context, ids map[string]bool, keep bool) (int, error) {
	return k.Sweep(ctx, func(id, _ string) bool { return ids[id] != keep }, false)
}

// Sweep lists the remembered fields and deletes every one drop selects, in
// Forget's order, returning how many deletes succeeded. With bestEffort a
// failed delete does not stop the sweep and the last such error is returned;
// otherwise it stops at the first error. A list error returns (0, err).
func (k Secrets) Sweep(ctx context.Context, drop func(id, field string) bool, bestEffort bool) (int, error) {
	index, err := k.Index(ctx)
	if err != nil {
		return 0, err
	}
	var order []string
	for id := range index {
		order = append(order, id)
	}
	sort.Strings(order)
	n := 0
	var failed error
	for _, id := range order {
		for _, f := range index[id] {
			if !drop(id, f) {
				continue
			}
			if err := k.Delete(ctx, id, f); err != nil {
				if !bestEffort {
					return n, err
				}
				failed = err
				continue
			}
			n++
		}
	}
	return n, failed
}
