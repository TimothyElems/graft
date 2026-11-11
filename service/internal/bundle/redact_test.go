package bundle

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

type redactionCases struct {
	Secrets []string `json:"secrets"`
	Cases   []struct {
		Name  string `json:"name"`
		Input string `json:"input"`
		Want  string `json:"want"`
	} `json:"cases"`
}

// The same file is checked by the Python SDK's test: two scrubbers, one rule set.
func TestRedactionSharedCases(t *testing.T) {
	raw, err := os.ReadFile("../../../fixtures/redaction/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var rc redactionCases
	if err := json.Unmarshal(raw, &rc); err != nil {
		t.Fatal(err)
	}
	r := NewRedactor(rc.Secrets...)
	for _, c := range rc.Cases {
		got, n := r.Redact(c.Input)
		if got != c.Want {
			t.Errorf("%s:\n input %q\n got   %q\n want  %q", c.Name, c.Input, got, c.Want)
		}
		if changed := got != c.Input; changed != (n > 0) {
			t.Errorf("%s: count %d but changed=%v", c.Name, n, changed)
		}
	}
}

// A literal secret too short to replace safely everywhere ("graft") is still caught
// where its shape gives it away, in a URL.
func TestShortSecretsAreNotReplacedEverywhere(t *testing.T) {
	r := NewRedactor("graft")
	got, _ := r.Redact("graft-fake ok:10 postgres://u:graft@h/db")
	if got != "graft-fake ok:10 postgres://u:[REDACTED]@h/db" {
		t.Fatalf("got %q", got)
	}
	if strings.Contains(got, ":graft@") {
		t.Fatal("password in url survived")
	}
}
