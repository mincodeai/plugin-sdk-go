package main

// Task-capable strict-family plugin recorded before its SDK migration:
// object-storage. Task actions talk to S3 directly (no host calls), so the
// task start payload fails validation deterministically.
func init() {
	plugins["object-storage"] = plugin{store: [2]string{"listProfiles", "{}"}, heldBusy: 4, busyCalls: 4, tasks: true, taskStart: [2]string{"deleteObjects", "{}"}}
}
