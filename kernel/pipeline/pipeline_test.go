package pipeline

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type fixture struct {
	Name   string          `json:"name"`
	Case   Case            `json:"case"`
	Expect json.RawMessage `json:"expect"`
}

func generic(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestFixtures(t *testing.T) {
	files, err := filepath.Glob("../../fixtures/pipeline/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("no fixtures found: %v", err)
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var fx fixture
		if err := json.Unmarshal(raw, &fx); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		t.Run(filepath.Base(f), func(t *testing.T) {
			var want any
			if err := json.Unmarshal(fx.Expect, &want); err != nil {
				t.Fatal(err)
			}
			res, err := Simulate(fx.Case)
			var got any
			if err != nil {
				got = map[string]any{"error": err.Error()}
			} else {
				got = generic(t, res)
			}
			if !reflect.DeepEqual(got, want) {
				g, _ := json.MarshalIndent(got, "", "  ")
				w, _ := json.MarshalIndent(want, "", "  ")
				t.Fatalf("mismatch\n got: %s\nwant: %s", g, w)
			}
		})
	}
}

func TestDeterministic(t *testing.T) {
	c := Case{
		Steps:       []Step{{ID: "a"}, {ID: "b"}, {ID: "c", Needs: []string{"a", "b"}}},
		Concurrency: 2,
	}
	first, _ := Simulate(c)
	for i := 0; i < 50; i++ {
		again, _ := Simulate(c)
		if !reflect.DeepEqual(first, again) {
			t.Fatal("simulation is not deterministic")
		}
	}
}
