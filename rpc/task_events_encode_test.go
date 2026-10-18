package rpc

import "testing"

func TestWithTaskEventsTouchesOnlyTheFeaturesObject(t *testing.T) {
	cases := []struct {
		name, in, want string
		sorted         bool
	}{
		{"plain", `{"features":{"tasks":1},"runtimeProtocolVersion":1}`, `{"features":{"tasks":1,"taskEvents":1},"runtimeProtocolVersion":1}`, false},
		{"sorted", `{"features":{"tasks":1}}`, `{"features":{"taskEvents":1,"tasks":1}}`, true},
		// StyleSorted database results write driver details before features.
		{"details first", `{"databaseDrivers":{"x":{"tasks":1}},"features":{"databaseDrivers":1,"tasks":1}}`,
			`{"databaseDrivers":{"x":{"tasks":1}},"features":{"databaseDrivers":1,"taskEvents":1,"tasks":1}}`, true},
		{"details only", `{"databaseDrivers":{"x":{"tasks":1}},"features":{"databaseDrivers":1}}`,
			`{"databaseDrivers":{"x":{"tasks":1}},"features":{"databaseDrivers":1}}`, true},
		{"no features", `{"tasks":1}`, `{"tasks":1}`, false},
	}
	for _, c := range cases {
		if got := string(withTaskEvents([]byte(c.in), c.sorted)); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}
