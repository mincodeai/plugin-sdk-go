// Command goldencmp checks that two golden trees record the same conversations
// up to line order: for every transcript, the sequence of inputs ("> " lines,
// "# eof", "# wait") must be identical and the stdout lines ("<" and "<~") and
// stderr lines ("!") must be equal as multisets.
//
//	go run ./internal/goldencmp [-missing-ok] OLD NEW
//
// Plugins or scenarios present on only one side are reported; with
// -missing-ok a plugin directory missing from OLD is not an error.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mincodeai/plugin-sdk-go/rpctest"
)

func main() {
	missingOK := flag.Bool("missing-ok", false, "plugin directories missing from OLD are not an error")
	flag.Parse()
	if flag.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "usage: goldencmp [-missing-ok] OLD NEW")
		os.Exit(2)
	}
	diffs, err := Compare(flag.Arg(0), flag.Arg(1), *missingOK)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	for _, d := range diffs {
		fmt.Println(d)
	}
	if len(diffs) > 0 {
		os.Exit(1)
	}
	fmt.Println("equivalent")
}

// Compare returns one message per difference between the golden trees.
func Compare(oldDir, newDir string, missingOK bool) ([]string, error) {
	oldFiles, err := list(oldDir)
	if err != nil {
		return nil, err
	}
	newFiles, err := list(newDir)
	if err != nil {
		return nil, err
	}
	oldPlugins := map[string]bool{}
	for f := range oldFiles {
		oldPlugins[filepath.Dir(f)] = true
	}
	var diffs []string
	for _, f := range union(oldFiles, newFiles) {
		switch {
		case !newFiles[f]:
			diffs = append(diffs, f+": missing from "+newDir)
			continue
		case !oldFiles[f]:
			if !(missingOK && !oldPlugins[filepath.Dir(f)]) {
				diffs = append(diffs, f+": missing from "+oldDir)
			}
			continue
		}
		a, err := rpctest.ReadFile(filepath.Join(oldDir, f))
		if err != nil {
			return nil, err
		}
		b, err := rpctest.ReadFile(filepath.Join(newDir, f))
		if err != nil {
			return nil, err
		}
		for _, d := range Diff(a, b) {
			diffs = append(diffs, f+": "+d)
		}
	}
	return diffs, nil
}

// Diff compares two transcripts up to output order.
func Diff(a, b rpctest.Transcript) []string {
	ai, ao, ae := split(a)
	bi, bo, be := split(b)
	var diffs []string
	if strings.Join(ai, "\n") != strings.Join(bi, "\n") {
		n := 0
		for n < len(ai) && n < len(bi) && ai[n] == bi[n] {
			n++
		}
		diffs = append(diffs, fmt.Sprintf("input sequence differs at input %d: %q vs %q", n+1, at(ai, n), at(bi, n)))
	}
	diffs = append(diffs, multiset("stdout", ao, bo)...)
	diffs = append(diffs, multiset("stderr", ae, be)...)
	return diffs
}

func at(s []string, i int) string {
	if i < len(s) {
		return clip(s[i])
	}
	return "<end>"
}

func clip(s string) string {
	if len(s) > 200 {
		return s[:200] + "…"
	}
	return s
}

func split(t rpctest.Transcript) (in, out, errs []string) {
	for _, l := range t {
		switch l.Kind {
		case rpctest.KindSend:
			in = append(in, "> "+l.Text)
		case rpctest.KindEOF:
			in = append(in, "# eof")
		case rpctest.KindWait:
			in = append(in, "# wait "+l.Text)
		case rpctest.KindExpect, rpctest.KindUnordered:
			out = append(out, l.Text)
		case rpctest.KindStderr:
			errs = append(errs, l.Text)
		}
	}
	return
}

func multiset(what string, a, b []string) []string {
	count := map[string]int{}
	for _, s := range a {
		count[s]++
	}
	for _, s := range b {
		count[s]--
	}
	var diffs []string
	for _, s := range sortedKeys(count) {
		switch n := count[s]; {
		case n > 0:
			diffs = append(diffs, fmt.Sprintf("%s line only in old (x%d): %s", what, n, clip(s)))
		case n < 0:
			diffs = append(diffs, fmt.Sprintf("%s line only in new (x%d): %s", what, -n, clip(s)))
		}
	}
	return diffs
}

func list(dir string) (map[string]bool, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*", "*.jsonl"))
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, f := range files {
		rel, _ := filepath.Rel(dir, f)
		out[rel] = true
	}
	return out, nil
}

func union(a, b map[string]bool) []string {
	m := map[string]int{}
	for k := range a {
		m[k] = 0
	}
	for k := range b {
		m[k] = 0
	}
	return sortedKeys(m)
}

func sortedKeys(m map[string]int) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
