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
