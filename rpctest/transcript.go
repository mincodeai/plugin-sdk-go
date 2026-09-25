package rpctest

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Transcript line kinds. A golden transcript is a linear script:
//
//	> {json}     line written to the plugin's stdin (a request or a host reply)
//	< {json}     next stdout line, compared byte for byte
//	<~ {json}    a run of adjacent "<~" lines is compared as a multiset
//	             (concurrent replies whose relative order is not deterministic)
//	! text       one stderr line; all stderr lines are compared as a multiset
//	# eof        close stdin
//	# wait 200ms pause before the next line (lets asynchronous work settle)
//	# anything   comment
//
// Input lines may contain @@PAD<n>@@, expanded to n 'x' bytes, so oversized
// frames need not be stored verbatim.
//
// Canonical layout (internal/recorder): every input line is followed by the
// stdout lines it caused, host replies directly after the host request they
// answer. Requests sent back to back on purpose (filling a busy gate) are
// written as consecutive "> " lines followed by one sorted "<~" block; replies
// drained by shutdown form a sorted "<~" block before the shutdown reply.
// Stderr lines are sorted.
const (
	KindSend      = '>'
	KindExpect    = '<'
	KindUnordered = '~'
	KindStderr    = '!'
	KindEOF       = 'E'
	KindWait      = 'W'
)

// Line is one parsed transcript line.
type Line struct {
	Kind byte
	Text string
}

// Transcript is an ordered list of lines.
type Transcript []Line

var padPattern = regexp.MustCompile(`@@PAD(\d+)@@`)

// ExpandPad replaces @@PAD<n>@@ markers with n 'x' bytes.
func ExpandPad(s string) string {
	return padPattern.ReplaceAllStringFunc(s, func(m string) string {
		n, _ := strconv.Atoi(padPattern.FindStringSubmatch(m)[1])
		return strings.Repeat("x", n)
	})
}

// Parse reads a transcript.
func Parse(data []byte) (Transcript, error) {
	var t Transcript
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 64*1024), 16<<20)
	for n := 1; sc.Scan(); n++ {
		s := sc.Text()
		switch {
		case s == "":
		case s == "# eof":
			t = append(t, Line{Kind: KindEOF})
		case strings.HasPrefix(s, "# wait "):
			if _, err := time.ParseDuration(s[7:]); err != nil {
				return nil, fmt.Errorf("line %d: %v", n, err)
			}
			t = append(t, Line{KindWait, s[7:]})
		case strings.HasPrefix(s, "#"):
		case strings.HasPrefix(s, "> "):
			t = append(t, Line{KindSend, s[2:]})
		case strings.HasPrefix(s, "<~ "):
			t = append(t, Line{KindUnordered, s[3:]})
		case strings.HasPrefix(s, "< "):
			t = append(t, Line{KindExpect, s[2:]})
		case strings.HasPrefix(s, "! "):
			t = append(t, Line{KindStderr, s[2:]})
		default:
			return nil, fmt.Errorf("line %d: unknown transcript line %q", n, s)
		}
	}
	return t, sc.Err()
}

// ReadFile parses a transcript file.
func ReadFile(path string) (Transcript, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

// Format renders the transcript in its file form.
func (t Transcript) Format() []byte {
	var b bytes.Buffer
	for _, l := range t {
		switch l.Kind {
		case KindEOF:
			b.WriteString("# eof")
		case KindWait:
			b.WriteString("# wait " + l.Text)
		case KindSend:
			b.WriteString("> " + l.Text)
		case KindExpect:
			b.WriteString("< " + l.Text)
		case KindUnordered:
			b.WriteString("<~ " + l.Text)
		case KindStderr:
			b.WriteString("! " + l.Text)
		}
		b.WriteByte('\n')
	}
	return b.Bytes()
}
