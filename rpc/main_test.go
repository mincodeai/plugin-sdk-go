package rpc

import (
	"io"
	"os"
	"testing"
)

func TestTakeStdio(t *testing.T) {
	stdin, stdout := os.Stdin, os.Stdout
	t.Cleanup(func() {
		if os.Stdin != nil && os.Stdin != stdin {
			_ = os.Stdin.Close()
		}
		os.Stdin, os.Stdout = stdin, stdout
	})
	in, out := takeStdio()
	if in != stdin || out != stdout {
		t.Fatal("takeStdio did not return the original standard input and output")
	}
	if os.Stdout != os.Stderr {
		t.Error("os.Stdout is not os.Stderr")
	}
	if n, err := os.Stdin.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Errorf("reading os.Stdin: %d, %v; want 0, EOF", n, err)
	}
}
