package runner

import (
	"strings"
	"testing"
	"time"
)

// Escape tests: each one TRIES to get out of the sandbox, from inside a real
// container, and requires that it fails. docs/threat-model.md lists what these do
// NOT cover (kernel exploits, side channels, ...).

func TestEscapeReachTheNetwork(t *testing.T) {
	d := dockerRunner(t)
	res := run(t, d, "run_esc_net", `
		wget -T 3 -q -O- http://1.1.1.1/ >/dev/null 2>&1; echo "ip=$?"
		nslookup -timeout=2 example.com >/dev/null 2>&1; echo "dns=$?"`, 20*time.Second)
	if !strings.Contains(res.Output, "ip=") || strings.Contains(res.Output, "ip=0") || strings.Contains(res.Output, "dns=0") {
		t.Fatalf("network reachable from the sandbox:\n%s", res.Output)
	}
}

func TestEscapeWriteToRootOrRunDownloadedCode(t *testing.T) {
	d := dockerRunner(t)
	res := run(t, d, "run_esc_fs", `
		touch /graft-escape 2>&1; echo "root=$?"
		echo 'echo pwned' > /tmp/x.sh && chmod +x /tmp/x.sh && /tmp/x.sh 2>&1; echo "exec=$?"
		echo ok > /tmp/scratch && echo "tmpwrite=$?"`, 20*time.Second)
	out := res.Output
	if !strings.Contains(out, "Read-only file system") || strings.Contains(out, "root=0") {
		t.Errorf("could write to the root filesystem:\n%s", out)
	}
	if strings.Contains(out, "pwned") || strings.Contains(out, "exec=0") {
		t.Errorf("could execute a file written to /tmp (noexec):\n%s", out)
	}
	if !strings.Contains(out, "tmpwrite=0") {
		t.Errorf("a step must still be able to use /tmp for scratch files:\n%s", out)
	}
}

// "Read a host path": the runner mounts no host paths at all. Prove it from inside:
// the only bind mounts are Docker's own hosts/hostname/resolv.conf, and the Docker
// socket (which would mean root on the host) is absent.
func TestEscapeReadAHostPath(t *testing.T) {
	d := dockerRunner(t)
	res := run(t, d, "run_esc_host", `
		awk '{print $5}' /proc/self/mountinfo
		test -e /var/run/docker.sock && echo SOCKET-PRESENT || echo socket-absent
		cat /etc/hostname /host/etc/passwd /mnt/etc/passwd 2>&1 | tail -2`, 20*time.Second)
	if !strings.Contains(res.Output, "socket-absent") {
		t.Fatalf("Docker socket visible inside the sandbox:\n%s", res.Output)
	}
	allowed := map[string]bool{"/etc/hosts": true, "/etc/hostname": true, "/etc/resolv.conf": true}
	for _, mp := range strings.Fields(res.Output) {
		if strings.HasPrefix(mp, "/etc/") && !allowed[mp] {
			t.Errorf("unexpected bind mount %s", mp)
		}
		if strings.HasPrefix(mp, "/host") || strings.HasPrefix(mp, "/mnt") || strings.HasPrefix(mp, "/var/run") {
			t.Errorf("host path mounted: %s", mp)
		}
	}
	if strings.Contains(res.Output, "root:x:0:0") {
		t.Fatalf("read a host passwd file:\n%s", res.Output)
	}
}

func TestEscapeForkBomb(t *testing.T) {
	d := dockerRunner(t)
	start := time.Now()
	res := run(t, d, "run_esc_fork", `
		bomb() { bomb | bomb & }; ( bomb ) 2>/dev/null &
		sleep 2
		echo "current=$(cat /sys/fs/cgroup/pids.current)"
		echo "events=$(cat /sys/fs/cgroup/pids.events)"`, 10*time.Second)
	t.Logf("fork bomb: %s after %s\n%s", res.Outcome, time.Since(start).Round(time.Millisecond), strings.TrimSpace(res.Output))
	if !strings.Contains(res.Output, "events=max ") || strings.Contains(res.Output, "events=max 0") {
		t.Fatalf("the pids limit was never hit (is --pids-limit applied?):\n%s", res.Output)
	}
	for _, line := range strings.Split(res.Output, "\n") {
		if n, ok := strings.CutPrefix(line, "current="); ok && len(n) > 2 {
			t.Fatalf("more than 99 processes in the sandbox: %s", n)
		}
	}
	noneLeft(t, d, "run_esc_fork")
	// The host is fine: the next container starts and runs normally.
	if after := run(t, d, "run_esc_after", "echo alive", 20*time.Second); after.Outcome != OK || after.Took > 10*time.Second {
		t.Fatalf("after the fork bomb: %+v", after)
	}
}

func TestEscapeGainPrivileges(t *testing.T) {
	d := dockerRunner(t)
	res := run(t, d, "run_esc_priv", `grep -E '^(CapEff|NoNewPrivs)' /proc/self/status; id -u`, 20*time.Second)
	if !strings.Contains(res.Output, "CapEff:\t0000000000000000") || !strings.Contains(res.Output, "NoNewPrivs:\t1") ||
		!strings.Contains(res.Output, "65534") {
		t.Fatalf("capabilities, no_new_privs or user not locked down:\n%s", res.Output)
	}
}

func TestEscapeMemoryBomb(t *testing.T) {
	d := dockerRunner(t)
	res := run(t, d, "run_esc_mem", `head -c 600m /dev/zero | tail > /dev/null; echo "survived=$?"`, 30*time.Second)
	if res.Outcome == OK && strings.Contains(res.Output, "survived=0") {
		t.Fatalf("allocated 600 MB under a 256 MB limit:\n%s", res.Output)
	}
	t.Logf("memory bomb: %s, exit %d, output %q", res.Outcome, res.ExitCode, strings.TrimSpace(res.Output))
}
