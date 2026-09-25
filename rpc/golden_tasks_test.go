package rpc_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// Task-capable golden presets recorded from the pre-SDK binaries:
// object-storage (task starts are admitted per action before the limits).
func init() {
	extraPresets["object-storage"] = func(*testing.T) rpc.Options {
		const invalid = "[S3_INPUT] Invalid input or unsupported field."
		o, st := strictPreset("object-storage", strictSpec{
			fatal: "object-storage plugin transport failed", busy: "Too many concurrent requests; try again later", invalidParams: invalid, maxInflight: 4,
			unknownAction: fixed("[S3_INPUT] Unknown action."), notObject: invalid, badJSON: invalid,
			unknownField: func(f string) string { return quoted("[S3_INPUT] Unsupported field ")(f) + "." },
			denied:       "[S3_STORAGE] Host storage is not permitted for this plugin.",
			failed:       "[S3_STORAGE] Host storage is unavailable; profiles were not changed.",
			secrets:      true, storeResult: `{"dropped":0,"max":50,"profiles":[]}`,
			extra: map[string]stubAction{"deleteObjects": {err: rpc.NewError(rpc.CodeInvalidParams, `[S3_INPUT] Missing required field "endpoint".`)}},
			tasks: &rpc.TaskOptions{
				Deadline:           290e9,
				SnapshotEscapeHTML: true,
				Admit: func(action string, _ json.RawMessage) *rpc.Error {
					if action != "deleteObjects" {
						return rpc.NewError(rpc.CodeInvalidParams, "[S3_INPUT] This action does not run as a task.")
					}
					return nil
				},
			},
		})
		o.WaitAll = true
		o.StructEnvelope, o.LenientErrorMember = true, true
		o.UnknownAction = func(string) *rpc.Error { return rpc.NewError(rpc.CodeInvalidParams, "[S3_INPUT] Unknown action.") }
		// Profile reads are not serialised: concurrent invokes each reach the host.
		o.Handler = func(ctx context.Context, action string, payload json.RawMessage) (any, error) {
			if action != "listProfiles" {
				return st.handle(ctx, action, payload)
			}
			if err := st.checkPayload(payload); err != nil {
				return nil, err
			}
			for _, c := range st.actions[action].calls {
				if _, err := host.FromContext(ctx).Call(ctx, c.method, c.params); err != nil {
					return nil, err
				}
			}
			return json.RawMessage(st.actions[action].result), nil
		}
		return o
	}
}
