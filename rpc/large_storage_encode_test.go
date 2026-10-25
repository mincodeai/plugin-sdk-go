package rpc

import "testing"

func TestWithFeatureKeepsOtherBytes(t *testing.T) {
	cases := []struct {
		name, in, want string
		sorted         bool
	}{
		{"no features", `{"runtimeProtocolVersion":1}`, `{"runtimeProtocolVersion":1,"features":{"largeStorage":1}}`, false},
		{"no features sorted", `{"runtimeProtocolVersion":1}`, `{"features":{"largeStorage":1},"runtimeProtocolVersion":1}`, true},
		{"appended", `{"runtimeProtocolVersion":1,"features":{"tasks":1,"taskEvents":1}}`, `{"runtimeProtocolVersion":1,"features":{"tasks":1,"taskEvents":1,"largeStorage":1}}`, false},
		{"sorted middle", `{"features":{"configuration":1,"taskEvents":1,"tasks":1},"runtimeProtocolVersion":1}`,
			`{"features":{"configuration":1,"largeStorage":1,"taskEvents":1,"tasks":1},"runtimeProtocolVersion":1}`, true},
		{"sorted first", `{"features":{"tasks":1},"runtimeProtocolVersion":1}`, `{"features":{"largeStorage":1,"tasks":1},"runtimeProtocolVersion":1}`, true},
		{"sorted last", `{"features":{"configuration":1},"runtimeProtocolVersion":1}`, `{"features":{"configuration":1,"largeStorage":1},"runtimeProtocolVersion":1}`, true},
		// StyleSorted database results write driver details before features.
		{"details first", `{"databaseDrivers":{"tasks":{"x":1}},"features":{"databaseDrivers":1,"tasks":1},"runtimeProtocolVersion":1}`,
			`{"databaseDrivers":{"tasks":{"x":1}},"features":{"databaseDrivers":1,"largeStorage":1,"tasks":1},"runtimeProtocolVersion":1}`, true},
	}
	for _, c := range cases {
		if got := string(withFeature([]byte(c.in), "largeStorage", c.sorted)); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.name, got, c.want)
		}
	}
}
