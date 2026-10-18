package host

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestGetConfiguration(t *testing.T) {
	var method string
	var params json.RawMessage
	caller := CallerFunc(func(_ context.Context, m string, p any) (json.RawMessage, error) {
		method = m
		params, _ = json.Marshal(p)
		return json.RawMessage(`{"values":{"endpoint":"https://example.test","retries":3,"token":"s3cret"}}`), nil
	})
	values, err := GetConfiguration(context.Background(), caller)
	if err != nil || method != "host.configuration.get" || string(params) != `{}` || string(values["retries"]) != "3" {
		t.Fatalf("get all: %s %s %v %v", method, params, values, err)
	}
	if _, err := GetConfiguration(context.Background(), caller, "endpoint"); err != nil || string(params) != `{"keys":["endpoint"]}` {
		t.Fatalf("get keys: %s %v", params, err)
	}
	var settings struct {
		Endpoint string `json:"endpoint"`
		Retries  int    `json:"retries"`
	}
	if err := DecodeConfiguration(context.Background(), caller, &settings); err != nil || settings.Endpoint != "https://example.test" || settings.Retries != 3 {
		t.Fatalf("decode: %+v %v", settings, err)
	}
	if _, err := GetConfiguration(context.Background(), nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("nil caller: %v", err)
	}
	bad := CallerFunc(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	if _, err := GetConfiguration(context.Background(), bad); err == nil {
		t.Fatal("missing values must fail")
	}
}
