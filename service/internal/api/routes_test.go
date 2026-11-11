package api

import (
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// Every operation the contract promises is served: without a token it must answer
// 401 (the route exists, auth runs first), never 404 or 405. healthz is public.
// An operation marked `x-graft-status: planned` is not built and is skipped; the
// SDK has the same gate (tests/unit/test_contract_coverage.py).
func TestEveryContractOperationIsRouted(t *testing.T) {
	raw, err := os.ReadFile("../../../contract/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	ts := newTest(t)
	param := regexp.MustCompile(`\{[a-z_]+\}`)
	n := 0
	for path, item := range spec.Paths {
		for method, v := range item {
			op, ok := v.(map[string]any)
			if !ok || method == "parameters" {
				continue
			}
			id, _ := op["operationId"].(string)
			if op["x-graft-status"] == "planned" {
				t.Logf("%s: planned, skipped", id)
				continue
			}
			n++
			req, _ := http.NewRequest(strings.ToUpper(method), ts.URL+param.ReplaceAllString(path, "x"), nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			want := http.StatusUnauthorized
			if id == "healthz" {
				want = http.StatusOK
			}
			if resp.StatusCode != want {
				t.Errorf("%s %s (%s): %d, want %d: the contract promises a route the server does not have",
					strings.ToUpper(method), path, id, resp.StatusCode, want)
			}
		}
	}
	if n < 9 {
		t.Fatalf("only %d operations checked; is the contract parsed?", n)
	}
}
