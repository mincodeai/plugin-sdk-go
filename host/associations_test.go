package host

import (
	"context"
	"encoding/json"
	"testing"
)

func TestAssociatedFileChunkValidation(t *testing.T) {
	c := CallerFunc(func(_ context.Context, method string, p any) (json.RawMessage, error) {
		if method != "host.associations.read" {
			t.Fatal(method)
		}
		return json.RawMessage(`{"data":"c2VlZA==","size":4,"eof":true}`), nil
	})
	b, e := ReadAssociatedFile(context.Background(), c, "request", 4)
	if e != nil || string(b) != "seed" {
		t.Fatal(string(b), e)
	}
	if _, e = ReadAssociatedFile(context.Background(), c, "request", 3); e == nil {
		t.Fatal("limit ignored")
	}
	if _, e = ReadAssociatedFile(context.Background(), nil, "request", 4); e == nil {
		t.Fatal("nil caller")
	}
}
