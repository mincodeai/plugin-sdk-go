package host

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
)

// Association describes only the calling plugin's declared handler.
type Association struct {
	HandlerID     string `json:"handlerId"`
	Type          string `json:"type"`
	Enabled       bool   `json:"enabled"`
	DefaultPlugin bool   `json:"defaultPlugin"`
	System        struct {
		Supported   bool   `json:"supported"`
		Default     bool   `json:"default"`
		Application string `json:"application"`
		Manual      bool   `json:"manual"`
	} `json:"system"`
}

func ListAssociations(ctx context.Context, c Caller) ([]Association, error) {
	if c == nil {
		return nil, ErrUnavailable
	}
	raw, e := c.Call(ctx, "host.associations.list", struct{}{})
	if e != nil {
		return nil, e
	}
	var result struct {
		Handlers []Association `json:"handlers"`
	}
	e = json.Unmarshal(raw, &result)
	return result.Handlers, e
}

// RequestAssociationSettings opens a trusted host dialog. Accepted is not a
// default-app success result; call ListAssociations again to observe the state.
func RequestAssociationSettings(ctx context.Context, c Caller, handlerID string) error {
	if c == nil {
		return ErrUnavailable
	}
	if handlerID == "" {
		return errors.New("handler id required")
	}
	_, e := c.Call(ctx, "host.associations.request", map[string]string{"handlerId": handlerID})
	return e
}

// ReadAssociatedFile reads one authorized external-open request in bounded
// chunks. The host rechecks grant, plugin declaration and file identity on
// every chunk; the plugin cannot supply an arbitrary path.
func ReadAssociatedFile(ctx context.Context, c Caller, id string, max int64) ([]byte, error) {
	if c == nil {
		return nil, ErrUnavailable
	}
	if id == "" || max < 1 || max > 16<<20 {
		return nil, errors.New("invalid file limit")
	}
	var data []byte
	var expected int64 = -1
	for {
		raw, e := c.Call(ctx, "host.associations.read", map[string]any{"requestId": id, "offset": len(data), "length": 64 << 10})
		if e != nil {
			return nil, e
		}
		var part struct {
			Data string `json:"data"`
			Size int64  `json:"size"`
			EOF  bool   `json:"eof"`
		}
		if e = json.Unmarshal(raw, &part); e != nil {
			return nil, e
		}
		if part.Size < 0 || part.Size > max || expected >= 0 && part.Size != expected {
			return nil, errors.New("file exceeds limit or changed")
		}
		expected = part.Size
		b, e := base64.StdEncoding.DecodeString(part.Data)
		if e != nil || len(b) > 64<<10 || int64(len(data)+len(b)) > part.Size {
			return nil, errors.New("invalid file chunk")
		}
		data = append(data, b...)
		if part.EOF {
			if int64(len(data)) != part.Size {
				return nil, errors.New("incomplete file")
			}
			return data, nil
		}
		if len(b) == 0 {
			return nil, errors.New("empty file chunk")
		}
	}
}
