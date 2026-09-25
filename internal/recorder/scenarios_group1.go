package main

// Strict-family plugins recorded before their SDK migration (group 1, no tasks).
func init() {
	plugins["mqtt-client"] = plugin{store: [2]string{"listProfiles", "{}"}, heldBusy: 16, local: [2]string{"sessions", "{}"}}
	plugins["openapi-contract"] = plugin{store: [2]string{"environments.list", "{}"}, heldBusy: 16}
}
