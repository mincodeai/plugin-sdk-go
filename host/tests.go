package host

import (
	"context"
	"errors"
)

// Streaming test runs (contributes.testProviders debugRun, host 0.3.0). The
// host calls the provider's resolveRunAction with {provider, project: {root},
// testIds, debug, runId}; the action returns the debug launch of the run
// ({variables?, request, arguments}, like a debuggers resolveAction) and the
// plugin then reports results for that runId with PublishTestEvents. When the
// run's debug session ends the plugin receives the host event
// EventTestRunEnded ({runId, reason}) in rpc Options.OnEvent, sent to the
// run's plugin whether or not it subscribed; late results are accepted for
// 10 seconds after it, a "finished" event closes the run at once.

// Test run event types (TestRunEvent.Type).
const (
	TestEventTree     = "tree"
	TestEventStarted  = "started"
	TestEventPassed   = "passed"
	TestEventFailed   = "failed"
	TestEventErrored  = "errored"
	TestEventSkipped  = "skipped"
	TestEventOutput   = "output"
	TestEventFinished = "finished"
)

// Test tree node kinds (TestRunEvent.Kind of a tree event).
const (
	TestKindSuite = "suite"
	TestKindTest  = "test"
)

// Host limits of PublishTestEvents.
const (
	TestPublishEventsMax = 500
	TestPublishMaxBytes  = 256 * 1024
	TestPublishPerSecond = 200
)

// ErrTestsUnsupported is returned by PublishTestEvents when the host has no
// host.tests.publish (hosts before 0.3.0) or the plugin declares no test
// provider with debugRun (-32601).
var ErrTestsUnsupported = errors.New("host does not support host.tests.publish")

// TestRunEvent is one generic test result event. Fields by type:
//
//   - tree: ID, Label, Kind (TestKindSuite or TestKindTest), optional
//     ParentID, Path (clean project-relative) and Line (needs Path); adds a
//     node, for example a parameterized invocation, to the run's tree;
//   - started: ID;
//   - passed, failed, errored, skipped: ID, optional DurationMs, Message
//     (≤ 4096 characters), Expected, Actual and Trace (each ≤ 64 KiB);
//   - output: Text (≤ 64 KiB), optional ID;
//   - finished: optional DurationMs; closes the run.
//
// Ids are test ids (≤ 256 bytes, no surrounding spaces or control
// characters), labels single lines of ≤ 256 characters. The host drops an
// event that breaks these rules (counted in TestPublishResult.Dropped) and
// accepts at most 10000 distinct ids per run.
type TestRunEvent struct {
	Type       string   `json:"type"`
	ID         string   `json:"id,omitempty"`
	ParentID   string   `json:"parentId,omitempty"`
	Label      string   `json:"label,omitempty"`
	Kind       string   `json:"kind,omitempty"`
	Path       string   `json:"path,omitempty"`
	Line       int      `json:"line,omitempty"`
	DurationMs *float64 `json:"durationMs,omitempty"`
	Message    string   `json:"message,omitempty"`
	Expected   *string  `json:"expected,omitempty"`
	Actual     *string  `json:"actual,omitempty"`
	Trace      string   `json:"trace,omitempty"`
	Text       string   `json:"text,omitempty"`
}

// TestPublishResult counts the events the host accepted and dropped.
type TestPublishResult struct {
	Accepted int `json:"accepted"`
	Dropped  int `json:"dropped"`
}

// TestRunEndedData is the EventTestRunEnded payload.
type TestRunEndedData struct {
	RunID  string `json:"runId"`
	Reason string `json:"reason"`
}

// PublishTestEvents calls host.tests.publish (host 0.3.0; needs the
// workspace.read capability and a test provider with debugRun): it reports
// events of the live run runID, which only this plugin's resolveRunAction
// received. At most TestPublishEventsMax events and TestPublishMaxBytes of
// params per call and TestPublishPerSecond calls per second (-32029 above
// it). A run that is unknown, another plugin's or has ended (finished, or 10
// seconds after its session ended) answers -32001.
func PublishTestEvents(ctx context.Context, c Caller, runID string, events []TestRunEvent) (TestPublishResult, error) {
	if c == nil {
		return TestPublishResult{}, ErrUnavailable
	}
	if runID == "" {
		return TestPublishResult{}, errors.New("test events require a run id")
	}
	if events == nil {
		events = []TestRunEvent{}
	}
	raw, err := c.Call(ctx, "host.tests.publish", map[string]any{"runId": runID, "events": events})
	if IsCode(err, -32601) {
		return TestPublishResult{}, ErrTestsUnsupported
	}
	if err != nil {
		return TestPublishResult{}, err
	}
	var result TestPublishResult
	return result, decodeInto(raw, &result)
}
