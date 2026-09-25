package main

// Node-compat plugins recorded before their SDK migration.
func init() {
	plugins["web-navigation"] = plugin{store: [2]string{"state", "{}"}, heldBusy: 1}
	plugins["prd-studio"] = plugin{store: [2]string{"prd.list", "{}"}, heldBusy: 1, tasks: true, taskStart: [2]string{"prd.list", "{}"}}
}
