package host

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Storage (manifest capability "storage"): bounded, non-sensitive JSON values
// and a plugin-owned file namespace. Never store credentials here; use the
// secrets functions instead.

// StorageFileChunkBytes is the per-call file chunk every host accepts; the
// SDK uses it with hosts that predate staged writes.
const StorageFileChunkBytes = 64 * 1024

// StorageFileStagedChunkBytes is the larger per-call chunk accepted by hosts
// that offer staged writes (host.storage.file.write.begin); the SDK switches
// to it for reads and writes after detecting that support.
const StorageFileStagedChunkBytes = 512 * 1024

// StorageFileMaxBytes is the host's per-file size limit.
const StorageFileMaxBytes = 64 * 1024 * 1024

// Staged write methods: begin opens a private staging file, append adds the
// next chunk, commit atomically replaces the target, abort discards it.
const (
	storageWriteBegin  = "host.storage.file.write.begin"
	storageWriteAppend = "host.storage.file.write.append"
	storageWriteCommit = "host.storage.file.write.commit"
	storageWriteAbort  = "host.storage.file.write.abort"
)

// ErrStagedWriteUnsupported is returned by WriteStorageFileAtomic when the
// host predates staged writes and the data does not fit in one call.
var ErrStagedWriteUnsupported = errors.New("host does not support staged storage writes")

// stagedStorageSupported asks host.api.describe whether staged writes are
// available. Any failure (older host, test double) means "not supported".
func stagedStorageSupported(ctx context.Context, c Caller) bool {
	description, err := Describe(ctx, c)
	return err == nil && description.Available(storageWriteBegin)
}

func call(ctx context.Context, c Caller, method string, params any) (json.RawMessage, error) {
	if c == nil {
		return nil, ErrUnavailable
	}
	return c.Call(ctx, method, params)
}

func decodeInto(raw json.RawMessage, target any) error {
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("decode host result: %w", err)
	}
	return nil
}

func isNull(raw json.RawMessage) bool { return len(raw) == 0 || string(raw) == "null" }

// GetStorage returns the stored JSON value; found is false when the key is unset.
func GetStorage(ctx context.Context, c Caller, key string) (value json.RawMessage, found bool, err error) {
	raw, err := call(ctx, c, "host.storage.get", map[string]string{"key": key})
	if err != nil || isNull(raw) {
		return nil, false, err
	}
	return raw, true, nil
}

// SetStorage stores a JSON-serializable value under key.
func SetStorage(ctx context.Context, c Caller, key string, value any) error {
	_, err := call(ctx, c, "host.storage.set", map[string]any{"key": key, "value": value})
	return err
}

// DeleteStorage removes key; deleting a missing key is not an error.
func DeleteStorage(ctx context.Context, c Caller, key string) error {
	_, err := call(ctx, c, "host.storage.delete", map[string]string{"key": key})
	return err
}

// ListStorageKeys returns the stored keys.
func ListStorageKeys(ctx context.Context, c Caller) ([]string, error) {
	raw, err := call(ctx, c, "host.storage.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var keys []string
	if isNull(raw) {
		return keys, nil
	}
	return keys, decodeInto(raw, &keys)
}

// ReadStorageFile reads a whole storage file in host-sized chunks. found is
// false when the file does not exist; files larger than maxBytes fail. Files
// larger than one 64 KiB chunk are read in 512 KiB chunks when the host
// supports them.
func ReadStorageFile(ctx context.Context, c Caller, path string, maxBytes int64) (data []byte, found bool, err error) {
	if maxBytes <= 0 || maxBytes > StorageFileMaxBytes {
		maxBytes = StorageFileMaxBytes
	}
	var offset int64
	chunkBytes, negotiated := StorageFileChunkBytes, false
	for {
		raw, err := call(ctx, c, "host.storage.file.read", map[string]any{"path": path, "offset": offset, "maxBytes": chunkBytes})
		if err != nil {
			return nil, false, err
		}
		if isNull(raw) {
			if offset > 0 {
				return nil, false, errors.New("storage file disappeared during read")
			}
			return nil, false, nil
		}
		var frame struct {
			Data       string `json:"data"`
			Offset     int64  `json:"offset"`
			NextOffset int64  `json:"nextOffset"`
			EOF        bool   `json:"eof"`
		}
		if err := decodeInto(raw, &frame); err != nil {
			return nil, false, err
		}
		chunk, err := base64.StdEncoding.DecodeString(frame.Data)
		if err != nil || frame.Offset != offset || frame.NextOffset != offset+int64(len(chunk)) || len(chunk) > chunkBytes {
			return nil, false, errors.New("host returned an invalid storage file frame")
		}
		data = append(data, chunk...)
		if int64(len(data)) > maxBytes {
			return nil, false, errors.New("storage file exceeds maxBytes")
		}
		offset = frame.NextOffset
		if frame.EOF {
			return data, true, nil
		}
		if len(chunk) == 0 {
			return nil, false, errors.New("host returned an empty non-terminal storage file frame")
		}
		if !negotiated {
			negotiated = true
			if stagedStorageSupported(ctx, c) {
				chunkBytes = StorageFileStagedChunkBytes
			}
		}
	}
}

// WriteStorageFile replaces a storage file. Data larger than one 64 KiB chunk
// uses a staged write when the host supports it, so the replacement is atomic
// and linear in the file size. With older hosts it falls back to sequential
// chunk writes, which are not atomic as a whole: a failure midway leaves a
// truncated file.
func WriteStorageFile(ctx context.Context, c Caller, path string, data []byte) error {
	if len(data) > StorageFileMaxBytes {
		return errors.New("storage file exceeds the host limit")
	}
	if len(data) > StorageFileChunkBytes && stagedStorageSupported(ctx, c) {
		return writeStorageFileStaged(ctx, c, path, data)
	}
	return writeStorageFileChunks(ctx, c, path, data)
}

// WriteStorageFileAtomic replaces a storage file atomically: readers see the
// old content or the complete new content, never a partial file. Data that
// fits in one 64 KiB call is written directly (each call is atomic); larger
// data needs a host with staged writes and otherwise fails with
// ErrStagedWriteUnsupported without touching the file.
func WriteStorageFileAtomic(ctx context.Context, c Caller, path string, data []byte) error {
	if len(data) > StorageFileMaxBytes {
		return errors.New("storage file exceeds the host limit")
	}
	if len(data) <= StorageFileChunkBytes {
		return writeStorageFileChunks(ctx, c, path, data)
	}
	if !stagedStorageSupported(ctx, c) {
		return ErrStagedWriteUnsupported
	}
	return writeStorageFileStaged(ctx, c, path, data)
}

func writeStorageFileStaged(ctx context.Context, c Caller, path string, data []byte) error {
	raw, err := call(ctx, c, storageWriteBegin, map[string]any{"path": path})
	if err != nil {
		return err
	}
	var begin struct {
		Session       string `json:"session"`
		MaxChunkBytes int    `json:"maxChunkBytes"`
	}
	if err := decodeInto(raw, &begin); err != nil {
		return err
	}
	if begin.Session == "" {
		return errors.New("host returned an invalid storage write session")
	}
	finished := false
	defer func() {
		if !finished {
			// Best effort: the host also drops idle sessions on its own.
			abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_, _ = call(abortCtx, c, storageWriteAbort, map[string]any{"session": begin.Session})
		}
	}()
	chunkBytes := begin.MaxChunkBytes
	if chunkBytes <= 0 {
		chunkBytes = StorageFileChunkBytes
	}
	chunkBytes = min(chunkBytes, StorageFileStagedChunkBytes)
	for start := 0; start < len(data); start += chunkBytes {
		end := min(start+chunkBytes, len(data))
		if _, err := call(ctx, c, storageWriteAppend, map[string]any{
			"session": begin.Session, "offset": start, "data": base64.StdEncoding.EncodeToString(data[start:end]),
		}); err != nil {
			return err
		}
	}
	raw, err = call(ctx, c, storageWriteCommit, map[string]any{"session": begin.Session, "size": len(data)})
	finished = err == nil // a failed commit may not have reached the host; abort is idempotent
	if err != nil {
		return err
	}
	if string(raw) != "true" {
		return errors.New("host did not confirm the storage file commit")
	}
	return nil
}

func writeStorageFileChunks(ctx context.Context, c Caller, path string, data []byte) error {
	for start := 0; start == 0 || start < len(data); start += StorageFileChunkBytes {
		end := min(start+StorageFileChunkBytes, len(data))
		raw, err := call(ctx, c, "host.storage.file.write", map[string]any{
			"path": path, "offset": start, "data": base64.StdEncoding.EncodeToString(data[start:end]), "truncate": start == 0,
		})
		if err != nil {
			return err
		}
		if string(raw) != "true" {
			return errors.New("host did not confirm the storage file write")
		}
		if len(data) == 0 {
			break
		}
	}
	return nil
}

// DeleteStorageFile removes a storage file.
func DeleteStorageFile(ctx context.Context, c Caller, path string) error {
	_, err := call(ctx, c, "host.storage.file.delete", map[string]string{"path": path})
	return err
}

// ListStorageFiles returns the file paths under prefix ("" for all).
func ListStorageFiles(ctx context.Context, c Caller, prefix string) ([]string, error) {
	raw, err := call(ctx, c, "host.storage.file.list", map[string]string{"path": prefix})
	if err != nil {
		return nil, err
	}
	var paths []string
	if isNull(raw) {
		return paths, nil
	}
	return paths, decodeInto(raw, &paths)
}

// StorageFileInfo describes a storage file; ModifiedAt is an RFC 3339 UTC time.
type StorageFileInfo struct {
	Size       int64  `json:"size"`
	ModifiedAt string `json:"modifiedAt"`
}

// StatStorageFile returns the file's size and modification time; found is
// false when it does not exist.
func StatStorageFile(ctx context.Context, c Caller, path string) (info StorageFileInfo, found bool, err error) {
	raw, err := call(ctx, c, "host.storage.file.stat", map[string]string{"path": path})
	if err != nil || isNull(raw) {
		return StorageFileInfo{}, false, err
	}
	return info, true, decodeInto(raw, &info)
}

// Secrets (manifest capability "secrets"): credentials only, encrypted with a
// machine-bound key, at most 256 keys of 64 KiB. Store them only after the
// user explicitly opted in.

// GetSecret returns the stored secret; found is false when the key is unset.
func GetSecret(ctx context.Context, c Caller, key string) (value string, found bool, err error) {
	raw, err := call(ctx, c, "host.secrets.get", map[string]string{"key": key})
	if err != nil || isNull(raw) {
		return "", false, err
	}
	return value, true, decodeInto(raw, &value)
}

// SetSecret stores a secret value.
func SetSecret(ctx context.Context, c Caller, key, value string) error {
	_, err := call(ctx, c, "host.secrets.set", map[string]string{"key": key, "value": value})
	return err
}

// DeleteSecret removes a secret.
func DeleteSecret(ctx context.Context, c Caller, key string) error {
	_, err := call(ctx, c, "host.secrets.delete", map[string]string{"key": key})
	return err
}

// ListSecrets returns the stored secret keys (never their values).
func ListSecrets(ctx context.Context, c Caller) ([]string, error) {
	raw, err := call(ctx, c, "host.secrets.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var keys []string
	if isNull(raw) {
		return keys, nil
	}
	return keys, decodeInto(raw, &keys)
}
