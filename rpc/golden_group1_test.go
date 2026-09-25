package rpc_test

import (
	"testing"

	"github.com/mincodeai/plugin-sdk-go/rpc"
)

// Group 1 golden presets (recorded from the pre-SDK binaries): mqtt-client and
// openapi-contract. Same family texts as nats-client.
func init() {
	const busy = "请求过多，请稍后重试 / Too many concurrent requests"
	extraPresets["mqtt-client"] = func(*testing.T) rpc.Options {
		const invalid = "[MQTT_INPUT] Invalid input or unsupported option."
		o, _ := strictPreset("mqtt-client", strictSpec{
			fatal: "MQTT plugin transport failed", busy: busy, invalidParams: invalid, maxInflight: 16, drain: true,
			unknownAction: quoted("[MQTT_INPUT] unknown action "), notObject: invalid, badJSON: invalid,
			unknownField: quoted("[MQTT_INPUT] unknown field "),
			denied:       "[MQTT_STORAGE] Host storage is not granted to this plugin.",
			failed:       "[MQTT_STORAGE] Host storage request failed.",
			secrets:      true, storeResult: `{"max":50,"profiles":[]}`,
			extra: map[string]stubAction{"sessions": {result: `{"max":8,"sessions":[]}`}},
		})
		o.StructEnvelope = true
		return o
	}
	extraPresets["openapi-contract"] = func(*testing.T) rpc.Options {
		const invalid = "[OPENAPI_INPUT] Invalid input or unsupported option."
		o, st := strictPreset("openapi-contract", strictSpec{
			fatal: "OpenAPI plugin transport failed", busy: busy, invalidParams: invalid, maxInflight: 16, drain: true,
			unknownAction: fixed("[OPENAPI_INPUT] unknown action"), notObject: invalid, badJSON: invalid,
			unknownField: quoted("[OPENAPI_INPUT] unknown field "),
			denied:       "[OPENAPI_STORAGE] Host storage is not granted to this plugin.",
			failed:       "[OPENAPI_STORAGE] Host storage request failed.",
		})
		delete(st.actions, "listProfiles")
		st.actions["environments.list"] = stubAction{calls: []hostCall{get("environments"), secretsList}, result: `{"environments":[],"max":50}`}
		o.StructEnvelope = true
		return o
	}
}
