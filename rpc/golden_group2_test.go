package rpc_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// Group 2 golden presets (recorded from the pre-SDK binaries): the Strict
// family plugins without tasks and without drained replies.
func init() {
	const storeResult = `{"dropped":0,"max":50,"profiles":[]}`
	extraPresets["rabbitmq-inspector"] = func(*testing.T) rpc.Options {
		const invalid = "[RABBITMQ_INPUT] invalid invoke parameters"
		o, st := strictPreset("rabbitmq-inspector", strictSpec{
			fatal: "rabbitmq-inspector: plugin transport failed", busy: "Too many RabbitMQ requests in flight; retry shortly", invalidParams: invalid, maxInflight: 4,
			unknownAction: fixed("[RABBITMQ_INPUT] unknown action"), notObject: "[RABBITMQ_INPUT] payload must be a JSON object",
			badJSON:      "[RABBITMQ_INPUT] payload must be a strict JSON object (UTF-8, no duplicate fields)",
			unknownField: quoted("[RABBITMQ_INPUT] invalid payload: json: unknown field "),
			denied:       "[RABBITMQ_STORAGE] Host storage is not granted to this plugin.",
			failed:       "[RABBITMQ_STORAGE] Host storage request failed.",
			secrets:      true, storeResult: storeResult,
		})
		o.Handler = concurrent(st)
		o.StructEnvelope = true
		return o
	}
	extraPresets["pulsar-inspector"] = func(*testing.T) rpc.Options {
		const invalid = "[PULSAR_INPUT] Invalid input or unsupported option."
		const badPayload = "[PULSAR_INPUT] Payload must be a JSON object without duplicate keys or invalid escapes."
		o, _ := strictPreset("pulsar-inspector", strictSpec{
			fatal: "pulsar-inspector: transport failed", busy: "Too many concurrent operations; retry shortly.", invalidParams: invalid, maxInflight: 4,
			unknownAction: fixed("[PULSAR_INPUT] Unknown action."), notObject: badPayload, badJSON: badPayload,
			unknownField: func(f string) string { return "[PULSAR_INPUT] Unsupported field " + quote(f) + "." },
			denied:       "[PULSAR_STORAGE] Plugin storage is not granted to this plugin.",
			failed:       "[PULSAR_STORAGE] Plugin storage request failed.",
			secrets:      true, storeResult: storeResult,
		})
		o.StructEnvelope = true
		return o
	}
	extraPresets["nacos-manager"] = func(*testing.T) rpc.Options {
		const invalid = "[NACOS_INPUT] Invalid input or unsupported option."
		o, st := strictPreset("nacos-manager", strictSpec{
			fatal: "Nacos plugin transport failed", busy: "已有多个请求运行，请稍后重试 / Too many concurrent requests", invalidParams: invalid, maxInflight: 4,
			unknownAction: fixed("[NACOS_INPUT] Invalid action."), notObject: invalid, badJSON: invalid,
			unknownField: func(f string) string { return "[NACOS_INPUT] Invalid field " + quote(f) + "." },
			denied:       "[NACOS_STORAGE] Profile storage permission is not granted for this plugin.",
			failed:       "[NACOS_STORAGE] Profile storage is unavailable or failed; try again. Passwords are never kept in profile storage.",
			secrets:      true, storeResult: storeResult,
		})
		o.Handler = concurrent(st)
		o.StructEnvelope = true
		return o
	}
	extraPresets["prometheus-query"] = func(*testing.T) rpc.Options {
		const invalid = "[PROM_INPUT] Invalid input or unsupported option."
		const badPayload = "[PROM_INPUT] Payload must be a JSON object without duplicate keys."
		o, _ := strictPreset("prometheus-query", strictSpec{
			fatal: "prometheus-query plugin transport failed", busy: "Too many concurrent requests; retry shortly", invalidParams: invalid, maxInflight: 6,
			unknownAction: fixed("[PROM_INPUT] Unknown action."), notObject: badPayload, badJSON: badPayload,
			// Only listProfiles is exercised with an unknown field.
			unknownField: func(f string) string { return "[PROM_INPUT] Unsupported field " + quote(f) + " for listProfiles." },
			denied:       "[PROM_STORAGE] Host storage is not granted to this plugin.",
			failed:       "[PROM_STORAGE] Host storage request failed.",
			secrets:      true, storeResult: storeResult,
		})
		o.StructEnvelope = true
		return o
	}
}

func quote(s string) string { return quoted("")(s) }

// concurrent runs each invoke on its own stub copy: plugins whose profile
// reads are not serialised by a store mutex (rabbitmq-inspector,
// nacos-manager) issue their host requests in parallel.
func concurrent(st *stub) rpc.Handler {
	return func(ctx context.Context, action string, payload json.RawMessage) (any, error) {
		c := &stub{actions: st.actions, checkPayload: st.checkPayload}
		return c.handle(ctx, action, payload)
	}
}
