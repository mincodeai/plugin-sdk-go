package main

// Strict-family plugins recorded after the first golden batch (no tasks).
func init() {
	plugins["rabbitmq-inspector"] = plugin{store: [2]string{"listProfiles", "{}"}, heldBusy: 4, busyCalls: 4}
	plugins["pulsar-inspector"] = plugin{store: [2]string{"listProfiles", "{}"}, heldBusy: 4}
	plugins["nacos-manager"] = plugin{store: [2]string{"listProfiles", "{}"}, heldBusy: 4, busyCalls: 4}
	plugins["prometheus-query"] = plugin{store: [2]string{"listProfiles", "{}"}, heldBusy: 6}
}
