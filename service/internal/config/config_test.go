package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func noEnv(string) string { return "" }

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "graft.toml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// The repo's graft.toml says "every key is shown with its default". Hold it to that.
func TestRepoGraftTomlEqualsTheDefaults(t *testing.T) {
	c, err := Load("../../../graft.toml", noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c, Defaults()) {
		t.Fatalf("graft.toml and Defaults() disagree:\n file:     %+v\n defaults: %+v", c, Defaults())
	}
}

func TestNoFileMeansDefaults(t *testing.T) {
	c, err := Load(filepath.Join(t.TempDir(), "absent.toml"), noEnv)
	if err != nil || !reflect.DeepEqual(c, Defaults()) {
		t.Fatalf("%v", err)
	}
}

func TestBadValuesAreRefusedWithTheKeyNamed(t *testing.T) {
	cases := map[string]string{
		"[workers]\nheartbeat = \"20s\"":       "workers.heartbeat: 20s must be positive and at most half of workers.lease (30s)",
		"[workers]\nlease = \"thirty\"":        `"thirty" is not a duration`,
		"[workers]\ncount = 0":                 "workers.count: 0 is out of range",
		"[runner]\nkind = \"kubernetes\"":      `runner.kind: "kubernetes" must be "script"`,
		"[runner]\nuser = \"0:0\"":             `runner.user: "0:0" runs steps as root`,
		"[runner]\nmemory = \"1m\"":            `runner.memory: "1m" must be a size like 256m, at least 6m`,
		"[runner]\npids = 0":                   "runner.pids: 0 is out of range",
		"[shutdown]\ngrace = \"30s\"":          "shutdown.deadline: 25s must be longer than shutdown.grace (30s)",
		"[server]\nmetrics_addr = \":8080\"":   "server.metrics_addr: must differ from server.addr",
		"[workers]\nheartbeet = \"5s\"":        "unknown key workers.heartbeet",
		"[server]\ntoken = \"hunter2hunter2\"": "server.token: secrets do not belong in a config file; set GRAFT_TOKEN instead",
		"[workers]\ncount = \"two\"":           "count", // a TOML type error names the key too
	}
	for body, want := range cases {
		_, err := Load(write(t, body), noEnv)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q:\n got:  %v\n want: ...%s...", body, err, want)
		}
	}
}

func TestAllProblemsAreReportedAtOnce(t *testing.T) {
	_, err := Load(write(t, "[workers]\ncount = 0\n[runner]\nkind = \"x\"\npids = 1"), noEnv)
	for _, key := range []string{"workers.count", "runner.kind", "runner.pids"} {
		if err == nil || !strings.Contains(err.Error(), key) {
			t.Errorf("missing %s in:\n%v", key, err)
		}
	}
}

func TestEnvOverridesTheFileAndIsValidatedToo(t *testing.T) {
	p := write(t, "[workers]\nlease = \"60s\"\nheartbeat = \"10s\"")
	env := map[string]string{"GRAFT_LEASE": "1500ms", "GRAFT_HEARTBEAT": "300ms", "GRAFT_WORKERS": "3"}
	c, err := Load(p, func(k string) string { return env[k] })
	if err != nil || c.Workers.Lease.Duration != 1500*time.Millisecond || c.Workers.Count != 3 {
		t.Fatalf("%+v %v", c.Workers, err)
	}
	env["GRAFT_HEARTBEAT"] = "1s" // more than half of the 1.5 s lease
	if _, err := Load(p, func(k string) string { return env[k] }); err == nil || !strings.Contains(err.Error(), "workers.heartbeat") {
		t.Fatalf("a bad env value must be refused too: %v", err)
	}
}

// The support bundle includes the configuration as JSON; durations must read as
// they were written in graft.toml ("30s"), not as nanoseconds.
func TestConfigAsJSONReadsLikeTheToml(t *testing.T) {
	b, err := json.Marshal(Defaults())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"workers":{"count":2,"lease":"30s"`) {
		t.Fatalf("durations in JSON: %s", b)
	}
}

// A mistyped variable (GRAFT_SERVER_ADDR for GRAFT_ADDR) used to be ignored in
// silence: the setting "did nothing". graftd now names it at startup.
func TestUnknownGraftVariablesAreNamed(t *testing.T) {
	asked := map[string]bool{}
	if _, err := Load(filepath.Join(t.TempDir(), "none.toml"), func(k string) string { asked[k] = true; return "" }); err != nil {
		t.Fatal(err)
	}
	got := UnknownEnv([]string{"GRAFT_SERVER_ADDR=127.0.0.1:1", "GRAFT_ADDR=:1", "PATH=/bin", "GRAFT_URL=http://x",
		"GRAFT_TOKEN=t", "GRAFT_WORKER=3"}, asked)
	if !reflect.DeepEqual(got, []string{"GRAFT_SERVER_ADDR", "GRAFT_WORKER"}) {
		t.Fatalf("unknown: %v", got)
	}
}
