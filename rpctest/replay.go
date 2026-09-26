package rpctest

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// ServeFunc runs a plugin transport over the given streams until stdin ends
// or the plugin stops.
type ServeFunc func(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer) error

// LineTimeout bounds how long Replay waits for one expected stdout line.
var LineTimeout = 5 * time.Second

// Replay drives serve with the transcript's inputs and fails t unless every
// stdout line matches byte for byte (multiset for "<~" runs), no extra line is
// written, and the stderr lines match as a multiset.
func Replay(t testing.TB, tr Transcript, serve ServeFunc) {
	t.Helper()
	stdin := NewPipe()
	outR, outW := io.Pipe()
	var errBuf lockedBuffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		err := serve(ctx, stdin, outW, &errBuf)
		_ = outW.Close()
		done <- err
	}()
	lines := make(chan string, 1024)
	go func() {
		sc := bufio.NewScanner(outR)
		sc.Buffer(make([]byte, 64*1024), 16<<20)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	next := func(i int) string {
		t.Helper()
		select {
		case s, ok := <-lines:
			if !ok {
				t.Fatalf("line %d: stdout closed, want %s", i+1, clip(tr[i].Text))
			}
			return s
		case <-time.After(LineTimeout):
			t.Fatalf("line %d: timed out waiting for %s", i+1, clip(tr[i].Text))
		}
		return ""
	}
	var wantErr []string
	lastSend := ""
	for i := 0; i < len(tr); i++ {
		l := tr[i]
		switch l.Kind {
		case KindSend:
			lastSend = ExpandPad(l.Text)
			_, _ = stdin.Write([]byte(lastSend + "\n"))
		case KindEOF:
			_ = stdin.Close()
		case KindWait:
			d, _ := time.ParseDuration(l.Text)
			time.Sleep(d)
		case KindStderr:
			wantErr = append(wantErr, l.Text)
		case KindExpect:
			got := next(i)
			// A task that is still running when its recorded status poll arrives
			// (slow or loaded machine) is polled again; status polls are read-only.
			for deadline := time.Now().Add(LineTimeout); !Match(l.Text, got) && isTaskStatusPoll(lastSend) &&
				strings.Contains(got, `"state":"running"`) && !strings.Contains(l.Text, `"state":"running"`) && time.Now().Before(deadline); {
				time.Sleep(20 * time.Millisecond)
				_, _ = stdin.Write([]byte(lastSend + "\n"))
				got = next(i)
			}
			if !Match(l.Text, got) {
				t.Fatalf("line %d mismatch\nwant %s\ngot  %s", i+1, clip(l.Text), clip(got))
			}
		case KindUnordered:
			j := i
			for j < len(tr) && tr[j].Kind == KindUnordered {
				j++
			}
			var want, got []string
			for k := i; k < j; k++ {
				want = append(want, tr[k].Text)
				got = append(got, next(k))
			}
			sort.Strings(want)
			sort.Strings(got)
			if !MatchSet(want, got) {
				t.Fatalf("lines %d-%d mismatch (unordered)\nwant %s\ngot  %s", i+1, j, clip(strings.Join(want, "\n")), clip(strings.Join(got, "\n")))
			}
			i = j - 1
		}
	}
	_ = stdin.Close()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("serve did not return after the transcript ended")
	}
	for s := range lines {
		t.Fatalf("unexpected extra stdout line %s", clip(s))
	}
	gotErr := strings.Split(strings.TrimSuffix(errBuf.String(), "\n"), "\n")
	if errBuf.String() == "" {
		gotErr = nil
	}
	sort.Strings(gotErr)
	sort.Strings(wantErr)
	if strings.Join(gotErr, "\n") != strings.Join(wantErr, "\n") {
		t.Fatalf("stderr mismatch\nwant %q\ngot  %q", wantErr, gotErr)
	}
}

func isTaskStatusPoll(line string) bool {
	return strings.Contains(line, `"method":"plugin.task.status"`)
}

func clip(s string) string {
	if len(s) > 600 {
		return s[:600] + "…"
	}
	return s
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// Pipe is an unbounded in-memory pipe: writes never block (like an OS pipe
// with a large buffer), reads block until data arrives or the pipe is closed.
type Pipe struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    bytes.Buffer
	closed bool
}

// NewPipe returns an open Pipe.
func NewPipe() *Pipe {
	p := &Pipe{}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Write appends p; it fails only after Close.
func (p *Pipe) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, io.ErrClosedPipe
	}
	p.buf.Write(b)
	p.cond.Broadcast()
	return len(b), nil
}

// Read blocks until data is available or the pipe is closed and drained.
func (p *Pipe) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.buf.Len() == 0 && !p.closed {
		p.cond.Wait()
	}
	if p.buf.Len() == 0 {
		return 0, io.EOF
	}
	return p.buf.Read(b)
}

// Close marks end of input; buffered data remains readable.
func (p *Pipe) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.cond.Broadcast()
	return nil
}

// Match reports whether got equals the golden line want, where each {{*}}
// placeholder in want matches any run of characters other than '"'.
func Match(want, got string) bool {
	if !strings.Contains(want, "{{*}}") {
		return want == got
	}
	parts := strings.Split(want, "{{*}}")
	for i, p := range parts {
		parts[i] = regexp.QuoteMeta(p)
	}
	return regexp.MustCompile("^" + strings.Join(parts, `(?:[^"\\]|\\.)*`) + "$").MatchString(got)
}

// MatchSet reports whether every want line matches a distinct got line.
func MatchSet(want, got []string) bool {
	if len(want) != len(got) {
		return false
	}
	used := make([]bool, len(got))
	// Exact lines first so placeholders cannot steal their partners.
	order := append([]string{}, want...)
	sort.SliceStable(order, func(a, b int) bool {
		return !strings.Contains(order[a], "{{*}}") && strings.Contains(order[b], "{{*}}")
	})
	for _, w := range order {
		found := false
		for k, g := range got {
			if !used[k] && Match(w, g) {
				used[k], found = true, true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
