// Package config loads graftd's settings: defaults, then graft.toml, then GRAFT_*
// environment variables, and validates the result. A bad value stops graftd at
// start with a message that names the key and says what is allowed.
//
// Secrets never live in the file: the token and database URL come only from
// GRAFT_TOKEN and GRAFT_DATABASE_URL.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// Duration is a TOML string like "30s" or "1500ms".
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return fmt.Errorf("%q is not a duration (examples: 500ms, 30s, 2m)", string(b))
	}
	d.Duration = v
	return nil
}

func (d Duration) String() string { return d.Duration.String() }

// MarshalText writes a duration the way graft.toml does ("30s"), for JSON too.
func (d Duration) MarshalText() ([]byte, error) { return []byte(d.Duration.String()), nil }

type Config struct {
	Server struct {
		Addr        string `toml:"addr" json:"addr"`
		MetricsAddr string `toml:"metrics_addr" json:"metrics_addr"`
	} `toml:"server" json:"server"`
	Workers struct {
		Count      int      `toml:"count" json:"count"`
		Lease      Duration `toml:"lease" json:"lease"`
		Heartbeat  Duration `toml:"heartbeat" json:"heartbeat"`
		CancelPoll Duration `toml:"cancel_poll" json:"cancel_poll"`
	} `toml:"workers" json:"workers"`
	Runner struct {
		Kind         string `toml:"kind" json:"kind"`
		DefaultImage string `toml:"default_image" json:"default_image"`
		Memory       string `toml:"memory" json:"memory"`
		CPUs         string `toml:"cpus" json:"cpus"`
		Pids         int    `toml:"pids" json:"pids"`
		User         string `toml:"user" json:"user"`
		TmpSize      string `toml:"tmp_size" json:"tmp_size"`
	} `toml:"runner" json:"runner"`
	Stream struct {
		WriteTimeout Duration `toml:"write_timeout" json:"write_timeout"`
		Poll         Duration `toml:"poll" json:"poll"`
	} `toml:"stream" json:"stream"`
	Shutdown struct {
		Grace    Duration `toml:"grace" json:"grace"`
		Deadline Duration `toml:"deadline" json:"deadline"`
	} `toml:"shutdown" json:"shutdown"`
}

func Defaults() Config {
	var c Config
	c.Server.Addr, c.Server.MetricsAddr = ":8080", ":9090"
	c.Workers.Count = 2
	c.Workers.Lease = Duration{30 * time.Second}
	c.Workers.Heartbeat = Duration{10 * time.Second}
	c.Workers.CancelPoll = Duration{time.Second}
	c.Runner.Kind, c.Runner.DefaultImage = "script", "busybox:1.37"
	c.Runner.Memory, c.Runner.CPUs, c.Runner.Pids = "256m", "1", 64
	c.Runner.User, c.Runner.TmpSize = "65534:65534", "16m"
	c.Stream.WriteTimeout, c.Stream.Poll = Duration{10 * time.Second}, Duration{2 * time.Second}
	c.Shutdown.Grace, c.Shutdown.Deadline = Duration{10 * time.Second}, Duration{25 * time.Second}
	return c
}

// secretKeys may appear by mistake; they get a message that says where secrets go.
var secretKeys = map[string]string{"token": "GRAFT_TOKEN", "database_url": "GRAFT_DATABASE_URL", "password": "GRAFT_DATABASE_URL"}

// Load reads path (if it exists; "" means no file), applies env overrides, validates.
func Load(path string, env func(string) string) (Config, error) {
	c := Defaults()
	if path != "" {
		if _, err := os.Stat(path); err == nil {
			md, err := toml.DecodeFile(path, &c)
			if err != nil {
				return c, fmt.Errorf("%s: %w", path, err)
			}
			var errs []error
			for _, k := range md.Undecoded() {
				name := k.String()
				last := name[strings.LastIndex(name, ".")+1:]
				if v, ok := secretKeys[last]; ok {
					errs = append(errs, fmt.Errorf("%s: %s: secrets do not belong in a config file; set %s instead", path, name, v))
				} else {
					errs = append(errs, fmt.Errorf("%s: unknown key %s (typo? see graft.toml in the repo for every key)", path, name))
				}
			}
			if len(errs) > 0 {
				return c, errors.Join(errs...)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return c, err
		}
	}
	if err := applyEnv(&c, env); err != nil {
		return c, err
	}
	return c, c.Validate()
}

func applyEnv(c *Config, env func(string) string) error {
	var errs []error
	str := func(k string, dst *string) {
		if v := env(k); v != "" {
			*dst = v
		}
	}
	dur := func(k string, dst *Duration) {
		if v := env(k); v != "" {
			if err := dst.UnmarshalText([]byte(v)); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", k, err))
			}
		}
	}
	str("GRAFT_ADDR", &c.Server.Addr)
	str("GRAFT_METRICS_ADDR", &c.Server.MetricsAddr)
	if v := env("GRAFT_WORKERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("GRAFT_WORKERS: %q is not a whole number", v))
		}
		c.Workers.Count = n
	}
	dur("GRAFT_LEASE", &c.Workers.Lease)
	dur("GRAFT_HEARTBEAT", &c.Workers.Heartbeat)
	str("GRAFT_RUNNER", &c.Runner.Kind)
	dur("GRAFT_SHUTDOWN_GRACE", &c.Shutdown.Grace)
	dur("GRAFT_SHUTDOWN_DEADLINE", &c.Shutdown.Deadline)
	return errors.Join(errs...)
}

var (
	sizeRe = regexp.MustCompile(`^([0-9]+)([kmg])$`)
	userRe = regexp.MustCompile(`^([0-9]+):([0-9]+)$`)
)

func sizeBytes(s string) (int64, bool) {
	m := sizeRe.FindStringSubmatch(strings.ToLower(s))
	if m == nil {
		return 0, false
	}
	n, _ := strconv.ParseInt(m[1], 10, 64)
	return n << map[string]uint{"k": 10, "m": 20, "g": 30}[m[2]], true
}

// Validate checks every value and reports ALL problems at once, each naming its key.
func (c Config) Validate() error {
	var errs []error
	bad := func(key, format string, a ...any) { errs = append(errs, fmt.Errorf(key+": "+format, a...)) }
	between := func(key string, d Duration, lo, hi time.Duration) {
		if d.Duration < lo || d.Duration > hi {
			bad(key, "%s is out of range (%s to %s)", d, lo, hi)
		}
	}
	for key, addr := range map[string]string{"server.addr": c.Server.Addr, "server.metrics_addr": c.Server.MetricsAddr} {
		if _, port, err := net.SplitHostPort(addr); err != nil || port == "" {
			bad(key, "%q is not host:port (example :8080)", addr)
		}
	}
	if c.Server.Addr == c.Server.MetricsAddr {
		bad("server.metrics_addr", "must differ from server.addr: metrics stay off the public API port")
	}
	if c.Workers.Count < 1 || c.Workers.Count > 64 {
		bad("workers.count", "%d is out of range (1 to 64)", c.Workers.Count)
	}
	between("workers.lease", c.Workers.Lease, 500*time.Millisecond, 10*time.Minute)
	if c.Workers.Heartbeat.Duration <= 0 || c.Workers.Heartbeat.Duration*2 > c.Workers.Lease.Duration {
		bad("workers.heartbeat", "%s must be positive and at most half of workers.lease (%s), so one slow beat does not lose the run",
			c.Workers.Heartbeat, c.Workers.Lease)
	}
	between("workers.cancel_poll", c.Workers.CancelPoll, 100*time.Millisecond, 10*time.Second)
	switch c.Runner.Kind {
	case "script", "docker":
	default:
		bad("runner.kind", "%q must be \"script\" (scripted steps only) or \"docker\" (real containers)", c.Runner.Kind)
	}
	if c.Runner.DefaultImage == "" {
		bad("runner.default_image", "must be set")
	}
	if n, ok := sizeBytes(c.Runner.Memory); !ok || n < 6<<20 {
		bad("runner.memory", "%q must be a size like 256m, at least 6m (Docker's minimum)", c.Runner.Memory)
	}
	if cpus, err := strconv.ParseFloat(c.Runner.CPUs, 64); err != nil || cpus <= 0 || cpus > 64 {
		bad("runner.cpus", "%q must be a number above 0 and at most 64", c.Runner.CPUs)
	}
	if c.Runner.Pids < 16 || c.Runner.Pids > 32768 {
		bad("runner.pids", "%d is out of range (16 to 32768); it is what stops a fork bomb", c.Runner.Pids)
	}
	if m := userRe.FindStringSubmatch(c.Runner.User); m == nil {
		bad("runner.user", "%q must be numeric uid:gid, like 65534:65534", c.Runner.User)
	} else if m[1] == "0" {
		bad("runner.user", "%q runs steps as root; untrusted steps must not run as root", c.Runner.User)
	}
	if _, ok := sizeBytes(c.Runner.TmpSize); !ok {
		bad("runner.tmp_size", "%q must be a size like 16m", c.Runner.TmpSize)
	}
	between("stream.write_timeout", c.Stream.WriteTimeout, time.Second, 5*time.Minute)
	between("stream.poll", c.Stream.Poll, 100*time.Millisecond, time.Minute)
	if c.Shutdown.Grace.Duration <= 0 {
		bad("shutdown.grace", "%s must be positive", c.Shutdown.Grace)
	}
	if c.Shutdown.Deadline.Duration <= c.Shutdown.Grace.Duration {
		bad("shutdown.deadline", "%s must be longer than shutdown.grace (%s): runs get the grace period, the process gets the deadline",
			c.Shutdown.Deadline, c.Shutdown.Grace)
	}
	return errors.Join(errs...)
}

// UnknownEnv returns GRAFT_* variables in environ that graftd does not read: most
// likely a typo. asked is every name Load looked up. Variables other Graft tools
// read (the SDK's GRAFT_URL, the tests' settings) are not reported.
func UnknownEnv(environ []string, asked map[string]bool) []string {
	var out []string
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(name, "GRAFT_") && !asked[name] && !otherTools[name] {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// Read by graftd outside Load (secrets, the -config default) or by other Graft tools.
var otherTools = map[string]bool{"GRAFT_TOKEN": true, "GRAFT_DATABASE_URL": true, "GRAFT_CONFIG": true,
	"GRAFT_URL": true, "GRAFT_TEST_DATABASE_URL": true, "GRAFT_DOCKER": true, "GRAFT_E2E": true}
