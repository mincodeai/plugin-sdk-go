package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/rpctest"
)

func parse(t *testing.T, s string) rpctest.Transcript {
	t.Helper()
	tr, err := rpctest.Parse([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestDiffIgnoresOutputOrder(t *testing.T) {
	a := parse(t, "> 1\n> 2\n< a\n< b\n# eof\n! x\n! y\n")
	b := parse(t, "> 1\n< b\n> 2\n<~ a\n# eof\n! y\n! x\n")
	if d := Diff(a, b); len(d) != 0 {
		t.Fatalf("Diff = %v", d)
	}
}

func TestDiffReportsContent(t *testing.T) {
	a := parse(t, "> 1\n> 2\n< a\n# eof\n")
	for _, c := range []struct{ src, want string }{
		{"> 2\n> 1\n< a\n# eof\n", "input sequence differs at input 1"},
		{"> 1\n> 2\n< a\n< a\n# eof\n", "only in new (x1): a"},
		{"> 1\n> 2\n# eof\n", "only in old (x1): a"},
		{"> 1\n> 2\n< a\n", "input sequence differs at input 3"},
	} {
		d := Diff(a, parse(t, c.src))
		if len(d) != 1 || !strings.Contains(d[0], c.want) {
			t.Errorf("Diff(%q) = %v, want %q", c.src, d, c.want)
		}
	}
}

func TestCompareTrees(t *testing.T) {
	root := t.TempDir()
	write := func(rel, s string) {
		p := filepath.Join(root, rel)
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("old/p/a.jsonl", "> 1\n< a\n")
	write("new/p/a.jsonl", "> 1\n< a\n")
	write("new/q/a.jsonl", "> 1\n")
	old, cur := filepath.Join(root, "old"), filepath.Join(root, "new")
	if d, err := Compare(old, cur, true); err != nil || len(d) != 0 {
		t.Fatalf("Compare missing-ok = %v, %v", d, err)
	}
	if d, _ := Compare(old, cur, false); len(d) != 1 {
		t.Fatalf("Compare = %v", d)
	}
}
