package jsvalue

import "testing"

func TestVal(t *testing.T) {
	o := O("a", 1.0, "b", nil)
	if o.Val("a") != 1.0 || o.Val("b") != nil || o.Val("missing") != nil || (*Obj)(nil).Val("a") != nil {
		t.Fatal("Val mismatch")
	}
}
