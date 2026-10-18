package host_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/host"
)

type recordedCall struct {
	method string
	params string
}

// scripted answers each method from a table and records the requests.
func scripted(t *testing.T, answers map[string]func(params map[string]any) (any, error)) (host.Caller, *[]recordedCall) {
	t.Helper()
	calls := &[]recordedCall{}
	return host.CallerFunc(func(_ context.Context, method string, params any) (json.RawMessage, error) {
		encoded, _ := json.Marshal(params)
		*calls = append(*calls, recordedCall{method, string(encoded)})
		answer, ok := answers[method]
		if !ok {
			t.Fatalf("unexpected call %s", method)
		}
		var decoded map[string]any
		_ = json.Unmarshal(encoded, &decoded)
		result, err := answer(decoded)
		if err != nil {
			return nil, err
		}
		return json.Marshal(result)
	}), calls
}

func TestStorageFilesChunkedRoundTrip(t *testing.T) {
	stored := map[string][]byte{}
	caller, calls := scripted(t, map[string]func(map[string]any) (any, error){
		// A host that predates staged writes: the SDK falls back to 64 KiB chunks.
		"host.api.describe": func(map[string]any) (any, error) {
			return map[string]any{"methods": []any{map[string]any{"name": "host.storage.file.write", "available": true}}}, nil
		},
		"host.storage.file.write": func(p map[string]any) (any, error) {
			chunk, _ := base64.StdEncoding.DecodeString(p["data"].(string))
			path := p["path"].(string)
			if p["truncate"] == true {
				stored[path] = nil
			}
			stored[path] = append(stored[path], chunk...)
			return true, nil
		},
		"host.storage.file.read": func(p map[string]any) (any, error) {
			data, ok := stored[p["path"].(string)]
			if !ok {
				return nil, nil
			}
			offset, max := int(p["offset"].(float64)), int(p["maxBytes"].(float64))
			end := min(offset+max, len(data))
			return map[string]any{"data": base64.StdEncoding.EncodeToString(data[offset:end]), "offset": offset, "nextOffset": end, "eof": end == len(data)}, nil
		},
	})
	payload := []byte(strings.Repeat("x", host.StorageFileChunkBytes*2+10))
	if err := host.WriteStorageFile(context.Background(), caller, "cache/a.bin", payload); err != nil {
		t.Fatal(err)
	}
	if len(*calls) != 4 || (*calls)[0].method != "host.api.describe" {
		t.Fatalf("write calls = %+v", *calls)
	}
	if err := host.WriteStorageFileAtomic(context.Background(), caller, "cache/b.bin", payload); !errors.Is(err, host.ErrStagedWriteUnsupported) {
		t.Fatalf("atomic write on an old host = %v", err)
	}
	if _, ok := stored["cache/b.bin"]; ok {
		t.Fatal("unsupported atomic write touched the file")
	}
	if err := host.WriteStorageFileAtomic(context.Background(), caller, "cache/small", []byte("tiny")); err != nil || string(stored["cache/small"]) != "tiny" {
		t.Fatalf("single-call atomic write = %v", err)
	}
	data, found, err := host.ReadStorageFile(context.Background(), caller, "cache/a.bin", 0)
	if err != nil || !found || string(data) != string(payload) {
		t.Fatalf("read = %d bytes found=%v err=%v", len(data), found, err)
	}
	if _, found, err := host.ReadStorageFile(context.Background(), caller, "missing", 0); err != nil || found {
		t.Fatalf("missing = %v %v", found, err)
	}
	if _, _, err := host.ReadStorageFile(context.Background(), caller, "cache/a.bin", 100); err == nil {
		t.Fatal("maxBytes not enforced")
	}
}

// stagedHost fakes a host with host.storage.file.write.begin/append/commit/abort.
func stagedHost(t *testing.T, failAppendAt int) (host.Caller, *[]recordedCall, map[string][]byte, map[string]bool) {
	stored, staging, aborted := map[string][]byte{}, map[string][]byte{}, map[string]bool{}
	targets := map[string]string{}
	appends := 0
	caller, calls := scripted(t, map[string]func(map[string]any) (any, error){
		"host.api.describe": func(map[string]any) (any, error) {
			return map[string]any{"methods": []any{map[string]any{"name": "host.storage.file.write.begin", "available": true}}}, nil
		},
		"host.storage.file.write.begin": func(p map[string]any) (any, error) {
			id := strings.Repeat("a", 31) + string(rune('0'+len(targets)))
			targets[id] = p["path"].(string)
			staging[id] = []byte{}
			return map[string]any{"session": id, "maxChunkBytes": 512 * 1024, "idleTimeoutMs": 60000}, nil
		},
		"host.storage.file.write.append": func(p map[string]any) (any, error) {
			id := p["session"].(string)
			appends++
			if appends == failAppendAt {
				return nil, &host.Error{Code: host.CodeStorageFailure, Message: "quota"}
			}
			if int(p["offset"].(float64)) != len(staging[id]) {
				t.Fatalf("append offset %v, staged %d", p["offset"], len(staging[id]))
			}
			chunk, _ := base64.StdEncoding.DecodeString(p["data"].(string))
			staging[id] = append(staging[id], chunk...)
			return map[string]any{"size": len(staging[id])}, nil
		},
		"host.storage.file.write.commit": func(p map[string]any) (any, error) {
			id := p["session"].(string)
			if int(p["size"].(float64)) != len(staging[id]) {
				return nil, &host.Error{Code: host.CodeStorageFailure, Message: "size"}
			}
			stored[targets[id]] = staging[id]
			delete(staging, id)
			return true, nil
		},
		"host.storage.file.write.abort": func(p map[string]any) (any, error) {
			aborted[p["session"].(string)] = true
			delete(staging, p["session"].(string))
			return true, nil
		},
		"host.storage.file.write": func(p map[string]any) (any, error) {
			chunk, _ := base64.StdEncoding.DecodeString(p["data"].(string))
			stored[p["path"].(string)] = chunk
			return true, nil
		},
		"host.storage.file.read": func(p map[string]any) (any, error) {
			data, ok := stored[p["path"].(string)]
			if !ok {
				return nil, nil
			}
			offset, max := int(p["offset"].(float64)), int(p["maxBytes"].(float64))
			end := min(offset+max, len(data))
			return map[string]any{"data": base64.StdEncoding.EncodeToString(data[offset:end]), "offset": offset, "nextOffset": end, "eof": end == len(data)}, nil
		},
	})
	return caller, calls, stored, aborted
}

func TestStorageFilesStagedWrite(t *testing.T) {
	caller, calls, stored, _ := stagedHost(t, 0)
	payload := []byte(strings.Repeat("0123456789", 160*1024)) // 1.6 MB: 4 appends of 512 KiB
	if err := host.WriteStorageFile(context.Background(), caller, "big.bin", payload); err != nil {
		t.Fatal(err)
	}
	methods := []string{}
	for _, call := range *calls {
		methods = append(methods, call.method)
	}
	want := "host.api.describe host.storage.file.write.begin host.storage.file.write.append host.storage.file.write.append host.storage.file.write.append host.storage.file.write.append host.storage.file.write.commit"
	if strings.Join(methods, " ") != want {
		t.Fatalf("calls = %v", methods)
	}
	if string(stored["big.bin"]) != string(payload) {
		t.Fatalf("stored %d bytes", len(stored["big.bin"]))
	}
	*calls = nil
	data, found, err := host.ReadStorageFile(context.Background(), caller, "big.bin", 0)
	if err != nil || !found || string(data) != string(payload) {
		t.Fatalf("read = %d %v %v", len(data), found, err)
	}
	// One 64 KiB probe, describe, then 512 KiB frames.
	if len(*calls) != 2+(len(payload)-host.StorageFileChunkBytes+host.StorageFileStagedChunkBytes-1)/host.StorageFileStagedChunkBytes {
		t.Fatalf("read calls = %d", len(*calls))
	}
	// Small files still use one direct call and no feature probe.
	*calls = nil
	if err := host.WriteStorageFile(context.Background(), caller, "small", []byte("x")); err != nil || len(*calls) != 1 || (*calls)[0].method != "host.storage.file.write" {
		t.Fatalf("small write = %v %+v", err, *calls)
	}
	if err := host.WriteStorageFileAtomic(context.Background(), caller, "atomic.bin", payload); err != nil || string(stored["atomic.bin"]) != string(payload) {
		t.Fatalf("atomic write = %v", err)
	}
}

func TestStorageFilesStagedWriteAbortsOnFailure(t *testing.T) {
	caller, _, stored, aborted := stagedHost(t, 2)
	payload := []byte(strings.Repeat("z", 3*512*1024))
	stored["big.bin"] = []byte("old")
	err := host.WriteStorageFileAtomic(context.Background(), caller, "big.bin", payload)
	if !host.IsCode(err, host.CodeStorageFailure) {
		t.Fatalf("err = %v", err)
	}
	if len(aborted) != 1 {
		t.Fatalf("aborted = %v", aborted)
	}
	if string(stored["big.bin"]) != "old" {
		t.Fatal("failed atomic write changed the file")
	}
}

func TestSecretsAndShell(t *testing.T) {
	caller, _ := scripted(t, map[string]func(map[string]any) (any, error){
		"host.secrets.get": func(p map[string]any) (any, error) {
			if p["key"] == "token" {
				return "s3cret", nil
			}
			return nil, nil
		},
		"host.shell.exec": func(p map[string]any) (any, error) {
			encoded, _ := json.Marshal(map[string]any{"success": false, "stdout": "", "stderr": "boom", "exit_code": 2})
			return string(encoded), nil
		},
	})
	if value, found, err := host.GetSecret(context.Background(), caller, "token"); err != nil || !found || value != "s3cret" {
		t.Fatalf("secret = %q %v %v", value, found, err)
	}
	if _, found, err := host.GetSecret(context.Background(), caller, "other"); err != nil || found {
		t.Fatalf("missing secret = %v %v", found, err)
	}
	result, err := host.Exec(context.Background(), caller, "false", host.ShellOptions{})
	if err != nil || result.Success || result.ExitCode != 2 || result.Stderr != "boom" {
		t.Fatalf("exec = %+v %v", result, err)
	}
}

func TestWorkspaceWrappers(t *testing.T) {
	caller, calls := scripted(t, map[string]func(map[string]any) (any, error){
		"host.workspace.root": func(map[string]any) (any, error) { return map[string]any{"root": nil}, nil },
		"host.workspace.write": func(p map[string]any) (any, error) {
			if p["expectedSha256"] == "" {
				return nil, &host.Error{Code: -32002, Message: "file already exists", Data: json.RawMessage(`{"code":"conflict"}`)}
			}
			return map[string]any{"path": p["path"], "size": 2, "sha256": "ab", "created": true}, nil
		},
		"host.workspace.read": func(p map[string]any) (any, error) {
			return map[string]any{"data": base64.StdEncoding.EncodeToString([]byte("hi")), "offset": 0, "nextOffset": 2, "eof": true, "size": 2}, nil
		},
	})
	if root, err := host.WorkspaceRoot(context.Background(), caller); err != nil || root != "" {
		t.Fatalf("root = %q %v", root, err)
	}
	if data, err := host.ReadWorkspaceFile(context.Background(), caller, "a.txt", 0); err != nil || string(data) != "hi" {
		t.Fatalf("read = %q %v", data, err)
	}
	_, err := host.WriteWorkspaceFile(context.Background(), caller, "a.txt", []byte("hi"), host.WorkspaceWriteOptions{CreateOnly: true})
	var hostErr *host.Error
	if !errors.As(err, &hostErr) || host.WorkspaceErrorCode(err) != "conflict" {
		t.Fatalf("create-only error = %v", err)
	}
	if result, err := host.WriteWorkspaceFile(context.Background(), caller, "b.txt", []byte("hi"), host.WorkspaceWriteOptions{CreateParents: true}); err != nil || !result.Created {
		t.Fatalf("write = %+v %v", result, err)
	}
	last := (*calls)[len(*calls)-1].params
	if strings.Contains(last, "expectedSha256") || !strings.Contains(last, `"createParents":true`) {
		t.Fatalf("write params = %s", last)
	}
	if _, err := host.WriteWorkspaceFile(context.Background(), caller, "big", make([]byte, host.WorkspaceWriteMaxBytes+1), host.WorkspaceWriteOptions{}); err == nil {
		t.Fatal("oversized write reached the host")
	}
}

func TestNilCallerIsUnavailable(t *testing.T) {
	if _, err := host.WorkspaceRoot(context.Background(), nil); !errors.Is(err, host.ErrUnavailable) {
		t.Fatalf("nil caller = %v", err)
	}
	if err := host.Log(context.Background(), nil, "info", "x"); !errors.Is(err, host.ErrUnavailable) {
		t.Fatalf("nil caller log = %v", err)
	}
}
