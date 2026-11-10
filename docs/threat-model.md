# Threat model: the step sandbox

Step code is untrusted: it comes from whoever can create a run. `graftd` runs each attempt with `runner.Docker` (`service/internal/runner/docker.go`). This page says what that stops, how we know, and, as important, what it does not stop.

**A container is not a virtual machine.** Every container shares the host's Linux kernel. Everything below narrows what step code can do; none of it removes the kernel as an attack surface.

## What it stops, and the test that proves it

Each test runs the attempt in a real container and tries to break out (`service/internal/runner/escape_test.go`, run in CI on every push).

| Attempt | Defence | Test result |
|---|---|---|
| Reach the network (IP or DNS) | `--network none`: only `lo` exists | both fail |
| Write to the image's filesystem | `--read-only` | "Read-only file system" |
| Download and run a binary | `/tmp` is the only writable path, mounted `noexec,nosuid,nodev`, 16 MB | executing it is refused |
| Read host files, use the Docker socket | no `-v`/`--mount`, ever; the only bind mounts are Docker's own `/etc/hosts`, `hostname`, `resolv.conf` | socket absent, no host paths |
| Fork bomb | `--pids-limit 64` | limit hit (151 times in one run), host unaffected, next container starts normally |
| Use all memory | `--memory 256m --memory-swap 256m` | killed by the OOM killer (exit 137) |
| Use all CPU | `--cpus 1` | not a test: `cpu.max` read back as one CPU |
| Be root, gain capabilities | `--user 65534:65534`, `--cap-drop ALL`, `--security-opt no-new-privileges` | `CapEff` 0, `NoNewPrivs` 1, uid 65534 |
| Outlive a cancel or timeout | the container is removed by name (`docker rm -f`), not just its client | none left when `Run` returns |
| Outlive a crashed worker | containers are labelled `graft.run=<id>`; the next claim removes them | `TestDockerCleanupRemovesOrphans` |

`TestDockerSandboxSettingsAreInEffect` reads the limits back from **inside** the container, so a flag that was passed but not enforced (no cgroup v2, an old Docker) fails the test rather than passing silently. `graft.toml` refuses `runner.user = "0:..."` at start.

## What it does not stop

- **Kernel exploits.** A bug in a system call the container can still make (and seccomp's default profile still allows) can give root on the host. This is the main reason a container is not a security boundary for hostile code. *Stronger:* a microVM per attempt (Firecracker, as AWS Lambda and Fargate use), gVisor (a user-space kernel), or a VM per job.
- **Container runtime bugs.** runc and containerd have had escape CVEs (for example CVE-2019-5736, CVE-2024-21626). Keep them patched; this repo cannot.
- **Side channels.** Steps share CPU caches and memory bandwidth with each other and with `graftd`. Timing attacks across containers are not addressed.
- **Disk exhaustion.** `/tmp` is capped, but container logs and Docker's own storage are not limited per attempt. A step that prints without end fills the daemon's log storage; `graftd` keeps only the last 8 KB of output, Docker keeps it all until the container is removed.
- **Denial of service inside the limits.** Sixty-four processes at 100% of one CPU each is allowed. Limits bound one attempt; nothing bounds how many runs a client creates (rate limiting is not built).
- **Secrets in images.** The runner pulls whatever `image` a step names, from any registry the daemon can reach. A step can name a malicious image; image allow-lists and signature checks are not built.
- **Pulling images.** The daemon (not the step) has network access to pull. A slow or huge pull ties up a worker; there is no pull timeout separate from the step's.
- **The Docker daemon itself.** `graftd` drives the daemon through the CLI. Whoever controls `graftd`'s host user controls the daemon, which is root-equivalent. Run `graftd` with the least access possible (rootless Docker or a remote daemon reduce this; not set up here).
- **At-least-once execution.** If a worker dies mid-run, another re-runs the pipeline from the start: steps can run twice (`run_started` says "restarted"). Non-idempotent steps (a migration that is not re-runnable) can do damage the second time. This is a correctness limit, not a sandbox one, but it belongs on this page.

## Where it runs

- **CI and local (Colima):** real Docker, every escape test runs.
- **The public demo on Fly:** Fly machines are Firecracker microVMs with no Docker daemon inside, so the demo uses `runner.kind = "script"` (scripted steps only). Running real step containers there would need nested virtualisation or a separate build host; not built.
