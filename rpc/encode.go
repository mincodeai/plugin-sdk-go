package rpc

import (
	"bytes"
	"encoding/json"

	"github.com/mincodeai/plugin-sdk-go/jsvalue"
)

type wireError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type goEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *wireError      `json:"error,omitempty"`
}

// encodeReply renders one reply line (without newline). result must be valid
// JSON when e is nil.
func (s *server) encodeReply(id json.RawMessage, result json.RawMessage, e *Error) ([]byte, error) {
	if s.o.Style != StyleJS {
		id = escapeIDSeparators(id)
	}
	var we *wireError
	if e != nil {
		we = &wireError{Code: e.Code, Message: e.Message, Data: e.Data}
		result = nil
	}
	switch s.o.Style {
	case StyleSorted:
		m := map[string]any{"jsonrpc": "2.0", "id": id}
		if we != nil {
			m["error"] = we
		} else {
			m["result"] = result
		}
		return encodeJSON(m, s.o.EscapeHTML)
	case StyleJS:
		var b bytes.Buffer
		b.WriteString(`{"jsonrpc":"2.0","id":`)
		v, err := jsvalue.Parse(id)
		if err != nil {
			return nil, err
		}
		jsvalue.Write(&b, v)
		if we != nil {
			b.WriteString(`,"error":{"code":`)
			jsvalue.Write(&b, float64(we.Code))
			b.WriteString(`,"message":`)
			jsvalue.WriteString(&b, we.Message)
			if len(we.Data) > 0 {
				b.WriteString(`,"data":`)
				b.Write(we.Data)
			}
			b.WriteString("}}")
		} else {
			b.WriteString(`,"result":`)
			b.Write(result)
			b.WriteByte('}')
		}
		return b.Bytes(), nil
	}
	return encodeJSON(goEnvelope{JSONRPC: "2.0", ID: id, Result: result, Error: we}, s.o.EscapeHTML)
}

// 仅规范化字符串 ID 的分隔符，保留数字精度与已有转义形式。
func escapeIDSeparators(id json.RawMessage) json.RawMessage {
	trimmed := bytes.TrimLeft(id, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '"' {
		return id
	}
	id = bytes.ReplaceAll(id, []byte("\u2028"), []byte(`\u2028`))
	return bytes.ReplaceAll(id, []byte("\u2029"), []byte(`\u2029`))
}

func encodeJSON(v any, escapeHTML bool) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(escapeHTML)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

// marshalResult turns a handler result into JSON bytes.
func (s *server) marshalResult(v any) (json.RawMessage, error) {
	switch x := v.(type) {
	case json.RawMessage:
		return x, nil
	case []byte:
		return x, nil
	case *jsvalue.Obj:
		return jsvalue.Stringify(x), nil
	}
	return encodeJSON(v, s.o.EscapeHTML)
}

// initializeResultFeatures is the plugin.initialize result. Without the
// configuration feature it is byte-identical to initializeResult.
func (s *server) initializeResultFeatures(tasks, configuration bool) json.RawMessage {
	if !configuration {
		return s.initializeResult(tasks)
	}
	switch {
	case tasks && s.o.Style == StyleSorted:
		return json.RawMessage(`{"features":{"configuration":1,"tasks":1},"runtimeProtocolVersion":1}`)
	case tasks:
		return json.RawMessage(`{"runtimeProtocolVersion":1,"features":{"tasks":1,"configuration":1}}`)
	case s.o.Style == StyleSorted:
		return json.RawMessage(`{"features":{"configuration":1},"runtimeProtocolVersion":1}`)
	}
	return json.RawMessage(`{"runtimeProtocolVersion":1,"features":{"configuration":1}}`)
}

// initializeResult is the plugin.initialize / plugin.ping result.
func (s *server) initializeResult(tasks bool) json.RawMessage {
	switch {
	case tasks && s.o.Style == StyleSorted:
		return json.RawMessage(`{"features":{"tasks":1},"runtimeProtocolVersion":1}`)
	case tasks:
		return json.RawMessage(`{"runtimeProtocolVersion":1,"features":{"tasks":1}}`)
	}
	return json.RawMessage(`{"runtimeProtocolVersion":1}`)
}

// initializeResultDatabase is the plugin.initialize result once
// features.databaseDrivers was negotiated: the negotiated features plus the
// handler's top-level "databaseDrivers" member (keys sorted for StyleSorted).
func (s *server) initializeResultDatabase(tasks, configuration bool) json.RawMessage {
	details := s.o.Database.DatabaseDetails()
	var b bytes.Buffer
	if s.o.Style == StyleSorted {
		b.WriteString(`{"databaseDrivers":`)
		b.Write(details)
		b.WriteString(`,"features":{`)
		if configuration {
			b.WriteString(`"configuration":1,`)
		}
		b.WriteString(`"databaseDrivers":1`)
		if tasks {
			b.WriteString(`,"tasks":1`)
		}
		b.WriteString(`},"runtimeProtocolVersion":1}`)
		return b.Bytes()
	}
	b.WriteString(`{"runtimeProtocolVersion":1,"features":{`)
	if tasks {
		b.WriteString(`"tasks":1,`)
	}
	if configuration {
		b.WriteString(`"configuration":1,`)
	}
	b.WriteString(`"databaseDrivers":1},"databaseDrivers":`)
	b.Write(details)
	b.WriteByte('}')
	return b.Bytes()
}

// withFeature adds "name":1 to the flat "features" object of an initialize
// result, or adds that object when there is none. The member goes last, or in
// key order for StyleSorted; every other byte is unchanged.
func withFeature(result json.RawMessage, name string, sorted bool) json.RawMessage {
	member := []byte(`"` + name + `":1`)
	const features = `"features":{`
	f := bytes.Index(result, []byte(features))
	if f < 0 {
		// No negotiated feature yet: {"runtimeProtocolVersion":1} gains one.
		end := bytes.LastIndexByte(result, '}')
		if end < 0 {
			return result
		}
		object := append(append([]byte(features), member...), '}')
		out := make([]byte, 0, len(result)+len(object)+1)
		if sorted {
			// "features" sorts before "runtimeProtocolVersion".
			out = append(out, '{')
			out = append(out, object...)
			out = append(out, ',')
			return append(out, result[1:]...)
		}
		out = append(out, result[:end]...)
		out = append(out, ',')
		out = append(out, object...)
		return append(out, result[end:]...)
	}
	start := f + len(features)
	end := bytes.IndexByte(result[start:], '}')
	if end < 0 {
		return result
	}
	end += start
	at := end
	if sorted {
		// Members are "key":1 in key order; insert before the first larger key.
		for i := start; i < end; {
			next := bytes.IndexByte(result[i:end], ',')
			key := result[i:end]
			if next >= 0 {
				key = result[i : i+next]
			}
			if k := bytes.IndexByte(key, ':'); k >= 0 && string(key[1:k-1]) > name {
				at = i
				break
			}
			if next < 0 {
				break
			}
			i += next + 1
		}
	}
	out := make([]byte, 0, len(result)+len(member)+1)
	out = append(out, result[:at]...)
	switch {
	case at == end && end == start:
		out = append(out, member...)
	case at == end:
		out = append(out, ',')
		out = append(out, member...)
	default:
		out = append(out, member...)
		out = append(out, ',')
	}
	return append(out, result[at:]...)
}

// withTaskEvents adds "taskEvents":1 next to the negotiated "tasks":1 member
// of an initialize result (before it for StyleSorted, whose keys are sorted),
// keeping every other byte unchanged.
func withTaskEvents(result json.RawMessage, sorted bool) json.RawMessage {
	const tasks = `"tasks":1`
	// Search only inside the flat "features" object: driver details or other
	// members may contain the same bytes (StyleSorted writes details first).
	const features = `"features":{`
	f := bytes.Index(result, []byte(features))
	if f < 0 {
		return result
	}
	start := f + len(features)
	end := bytes.IndexByte(result[start:], '}')
	if end < 0 {
		return result
	}
	j := bytes.Index(result[start:start+end], []byte(tasks))
	if j < 0 {
		return result
	}
	i := start + j
	insert := `"tasks":1,"taskEvents":1`
	if sorted {
		insert = `"taskEvents":1,"tasks":1`
	}
	out := make([]byte, 0, len(result)+len(`,"taskEvents":1`))
	out = append(out, result[:i]...)
	out = append(out, insert...)
	return append(out, result[i+len(tasks):]...)
}
