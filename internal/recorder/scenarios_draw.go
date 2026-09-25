package main

// draw (standalone gitlink repository, vendors this SDK) shares Excalidraw's
// Node-compat runtime and its recent.get / recent.set / file.save actions.
// It has no golden files; the scenarios exist to prove a migrated binary
// replies byte-for-byte like the pre-migration one (record both, then diff or
// run internal/goldencmp).
func init() {
	plugins["draw"] = plugin{
		store:    [2]string{"recent.get", "{}"},
		local:    [2]string{"file.save", `{"filename":"a.drawio","content":"<mxfile/>"}`},
		heldBusy: 1,
	}
}
