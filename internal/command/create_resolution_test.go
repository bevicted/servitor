package command

import "testing"

func TestParseCreateOptionsDoesNotApplyDefaultsUntilResolution(t *testing.T) {
	options, err := ParseCreateOptions("create --version 4.22")
	if err != nil {
		t.Fatal(err)
	}
	if got := one(options.values, "--target"); got != "" {
		t.Fatalf("target = %q, want explicit-only empty", got)
	}
	resolved, err := ResolveCreateOptions(options, testCreateDefaults)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Target != testCreateDefaults.Target || resolved.Version != "4.22" {
		t.Fatalf("resolved = %+v", resolved)
	}
}
