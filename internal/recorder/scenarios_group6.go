package main

// Group 6 plugins recorded before their SDK migration: etcd-manager (strict,
// no live etcd needed: profile actions talk to the fake host, "sessions" is
// local) and network-diagnostics (Node-style payloads, tasks; tcpCheck {}
// fails validation deterministically).
func init() {
	plugins["etcd-manager"] = plugin{store: [2]string{"listProfiles", "{}"}, local: [2]string{"sessions", "{}"}, heldBusy: 8}
	plugins["network-diagnostics"] = plugin{store: [2]string{"listTargets", "{}"}, local: [2]string{"tcpCheck", "{}"}, heldBusy: 4, tasks: true, taskStart: [2]string{"listTargets", "{}"}}
}
