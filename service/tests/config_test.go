package tests

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// graftd must refuse to start on bad configuration, before touching the database,
// and say exactly which key is wrong.
func TestGraftdRefusesBadConfigWithAClearMessage(t *testing.T) {
	bad := filepath.Join(t.TempDir(), "graft.toml")
	if err := os.WriteFile(bad, []byte("[workers]\nheartbeat = \"20s\"\n[runner]\nuser = \"0:0\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(graftd, "-config", bad)
	cmd.Env = append(os.Environ(), "GRAFT_TOKEN="+token, "GRAFT_DATABASE_URL=postgres://nowhere:1/x")
	out, err := cmd.CombinedOutput()
	t.Logf("graftd said:\n%s", out)
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 2 {
		t.Fatalf("exit %v, want 2", err)
	}
	for _, want := range []string{"invalid configuration, not starting", "workers.heartbeat", "runner.user", "root"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("message missing %q", want)
		}
	}
}

func TestGraftdRefusesABadEnvOverride(t *testing.T) {
	cmd := exec.Command(graftd, "-config", filepath.Join(t.TempDir(), "none.toml"))
	cmd.Env = append(os.Environ(), "GRAFT_TOKEN="+token, "GRAFT_DATABASE_URL=postgres://nowhere:1/x", "GRAFT_RUNNER=kubernetes")
	out, err := cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 2 || !strings.Contains(string(out), "runner.kind") {
		t.Fatalf("exit %v, output:\n%s", err, out)
	}
}
