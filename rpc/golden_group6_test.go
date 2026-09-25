package rpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/mincodeai/plugin-sdk-go/host"
	"github.com/mincodeai/plugin-sdk-go/jsonx"
	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// Group 6 golden presets (recorded from the pre-SDK binaries): etcd-manager
// (strict family, unknown actions answered by the plugin, unbounded drain)
// and network-diagnostics (Go struct envelope with Node-style payloads, reply
// line limit, tasks validated like invokes after the id).
func init() {
	extraPresets["etcd-manager"] = func(*testing.T) rpc.Options {
		const invalid = "[ETCD_INPUT] Invalid input or unsupported option."
		o, st := strictPreset("etcd-manager", strictSpec{
			fatal: "ETCD plugin transport failed", busy: "已有任务运行，请稍后重试", invalidParams: invalid, maxInflight: 8,
			notObject: invalid, badJSON: invalid, unknownField: func(f string) string { return quoted("[ETCD_INPUT] Unsupported or null field: ")(f) + "." },
			unknownAction: fixed("[ETCD_INPUT] Unknown action."),
			denied:        "[ETCD_STORAGE] Plugin storage is not granted to this plugin.",
			failed:        "[ETCD_STORAGE] Plugin storage is unavailable; profiles need the MinCode desktop app.",
			secrets:       true, storeResult: `{"connected":[],"max":50,"profiles":[]}`,
			extra: map[string]stubAction{"sessions": {result: `{"max":8,"sessions":[]}`}},
		})
		unknown := fixedErr(rpc.CodeInvalidParams, "[ETCD_INPUT] Unknown action.")
		o.KnownAction = nil
		o.Handler = func(ctx context.Context, action string, payload json.RawMessage) (any, error) {
			if !st.known(action) {
				return nil, unknown
			}
			return st.handle(ctx, action, payload)
		}
		o.WaitAll = true
		o.StructEnvelope = true
		o.Host.EscapeHTML = true
		return o
	}
	extraPresets["network-diagnostics"] = func(*testing.T) rpc.Options {
		const oversized = "结果过大，请缩小查询范围"
		o := rpc.NodeDefaults()
		o.RequireVersion = true
		o.StructEnvelope = true
		o.StructParams = true
		o.StrictPayload = true
		o.IDs = rpc.IDNonNull
		o.NonStringMethod = rpc.MethodTypeInvalid
		o.FatalLineBytes = 4 << 20
		o.MaxInflight = 4
		o.MaxReplyBytes = 220000
		o.ResultTooLarge = rpc.NewError(rpc.CodeServerError, oversized)
		o.PanicError = rpc.NewError(rpc.CodeServerError, "插件调用失败")
		o.ShutdownReplyLast = true
		o.RejectTaskMethods = true
		o.MapError = func(err error) *rpc.Error {
			switch {
			case host.IsCode(err, host.CodeCapabilityMissing):
				return rpc.NewError(rpc.CodeServerError, "宿主未授予插件存储权限")
			case errors.As(err, new(*host.Error)):
				return rpc.NewError(rpc.CodeServerError, "宿主存储请求失败")
			}
			return nil
		}
		o.Host.IDPrefix = "network-diagnostics-host-"
		o.Host.MaxLineBytes = 1 << 20
		o.Tasks = &rpc.TaskOptions{
			MaxRetained: 64, MaxRunning: 2, Deadline: 290 * time.Second, Retention: 5 * time.Minute,
			MaxResultBytes: 240000, MaxProgressBytes: 4096, MaxErrorBytes: 1000,
			LimitReached: rpc.NewError(rpc.CodeServerError, "任务数量已达上限"),
			Busy:         rpc.NewError(rpc.CodeBusy, "已有任务运行，请稍后重试"),
			Cancelled:    "任务已取消", Timeout: "任务超时", ResultTooLarge: oversized,
			CheckPayload: true, ClearProgressOnSuccess: true, SnapshotEscapeHTML: true,
		}
		st := &stub{actions: map[string]stubAction{
			"listTargets": {calls: []hostCall{get("targets")}, result: `{"dropped":0,"max":50,"targets":[]}`},
			"tcpCheck":    {err: rpc.NewError(rpc.CodeServerError, "请输入主机名或 IP 地址")},
		}}
		st.checkPayload = func(p json.RawMessage) error {
			var empty struct{}
			err := jsonx.DecodeObject(p, &empty, jsonx.DecodeOptions{DisallowUnknown: true})
			var de *jsonx.DecodeError
			if errors.As(err, &de) && de.Kind == jsonx.UnknownField {
				return rpc.NewError(rpc.CodeServerError, quoted("不支持的参数 ")(de.Field))
			}
			return err
		}
		o.KnownAction = st.known
		o.Handler = st.handle
		return o
	}
}

func fixedErr(code int, msg string) *rpc.Error { return rpc.NewError(code, msg) }
