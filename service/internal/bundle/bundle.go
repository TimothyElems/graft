// Package bundle builds a run's support bundle: one .tar.gz with everything needed
// to understand the run and replay it in the kernel, with secrets removed.
//
//	manifest.json              what is inside, how many secrets were removed, caveats
//	run.json                   the run: status, spec, claims, error, step results
//	events.json                every event, every claim, in seq order
//	case.json                  the kernel Case that reproduces the last claim's events
//	                           (absent if the run has not finished; manifest says why)
//	attempts/claim-N/S-attempt-K.log   what each attempt printed (last 8 KB)
//	config.json                the service's settings (graft.toml holds no secrets)
//
// Replay: scripts/replay.sh <bundle> (docs/replay.md).
package bundle

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"graft/kernel/pipeline"
	"graft/service/internal/db"
	"graft/service/internal/events"
	"graft/service/internal/sched"
	"graft/service/internal/store"
)

// Format names this layout. Bump it when a file changes meaning.
const Format = "graft-bundle/1"

// MaxEvents bounds events.json. A run has a few events per attempt; this is far above that.
const MaxEvents = 100_000

type Options struct {
	Version string
	Config  any      // the loaded configuration
	Secrets []string // literal values to remove wherever they appear
	Now     func() time.Time
}

type File struct {
	Name  string `json:"name"`
	Bytes int    `json:"bytes"`
}

type Manifest struct {
	Format         string    `json:"format"`
	RunID          string    `json:"run_id"`
	CreatedAt      time.Time `json:"created_at"`
	ServiceVersion string    `json:"service_version"`
	Files          []File    `json:"files"`
	Redactions     int       `json:"redactions"`
	Notes          []string  `json:"notes"`
}

const scrubNote = "Secrets were removed by value (this service's API token and database password) and by shape " +
	"(fixtures/redaction/cases.json). That is best effort: a secret a step printed in an unknown shape can remain. " +
	"Read the attempt logs before you share this file."

var unsafeName = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// Build returns runID's bundle. store.ErrNotFound if there is no such run.
func Build(ctx context.Context, q db.DBTX, runID string, o Options) ([]byte, Manifest, error) {
	now := time.Now
	if o.Now != nil {
		now = o.Now
	}
	run, err := store.GetRun(ctx, q, runID)
	if err != nil {
		return nil, Manifest{}, err
	}
	var claims int
	var cancelRequested bool
	var errText, owner *string
	var leaseUntil *time.Time
	if err := q.QueryRow(ctx, `SELECT claims, cancel_requested, error, lease_owner, lease_until FROM runs WHERE id = $1`, runID).
		Scan(&claims, &cancelRequested, &errText, &owner, &leaseUntil); err != nil {
		return nil, Manifest{}, err
	}
	evs, err := events.List(ctx, q, runID, 0, MaxEvents)
	if err != nil {
		return nil, Manifest{}, err
	}

	m := Manifest{Format: Format, RunID: runID, CreatedAt: now().UTC(), ServiceVersion: o.Version, Notes: []string{scrubNote}}
	type file struct {
		name string
		body []byte
	}
	var files []file
	add := func(name string, v any) error {
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		files = append(files, file{name, append(b, '\n')})
		return nil
	}

	steps := make([]map[string]any, len(run.Steps))
	for i, s := range run.Steps {
		steps[i] = map[string]any{"id": s.ID, "status": s.Status, "attempts": s.Attempts,
			"started_ms": s.StartedMs, "finished_ms": s.FinishedMs, "reason": s.Reason}
	}
	if err := add("run.json", map[string]any{"id": run.ID, "branch_id": run.BranchID, "status": run.Status,
		"created_at": run.CreatedAt, "finished_at": run.FinishedAt, "claims": claims, "cancel_requested": cancelRequested,
		"error": errText, "lease_owner": owner, "lease_until": leaseUntil, "spec": run.Spec, "steps": steps}); err != nil {
		return nil, Manifest{}, err
	}
	if len(evs) == MaxEvents {
		m.Notes = append(m.Notes, fmt.Sprintf("events.json is cut at %d events.", MaxEvents))
	}
	if err := add("events.json", evs); err != nil {
		return nil, Manifest{}, err
	}
	if c, err := CaseFor(run.Spec, evs); err != nil {
		m.Notes = append(m.Notes, "case.json is not included: "+err.Error())
	} else {
		c.Name = "bundle " + runID
		if err := add("case.json", c); err != nil {
			return nil, Manifest{}, err
		}
	}

	rows, err := q.Query(ctx, `SELECT claim, step_id, attempt, outcome, exit_code, took_ms, output
		FROM attempt_logs WHERE run_id = $1 ORDER BY claim, step_id, attempt`, runID)
	if err != nil {
		return nil, Manifest{}, err
	}
	for rows.Next() {
		var claim, attempt, exit int
		var step, outcome, output string
		var took int64
		if err := rows.Scan(&claim, &step, &attempt, &outcome, &exit, &took, &output); err != nil {
			rows.Close()
			return nil, Manifest{}, err
		}
		body := fmt.Sprintf("# step %s, attempt %d (claim %d)\n# outcome %s, exit code %d, took %d ms\n%s",
			step, attempt, claim, outcome, exit, took, output)
		files = append(files, file{fmt.Sprintf("attempts/claim-%d/%s-attempt-%d.log", claim, unsafeName.ReplaceAllString(step, "_"), attempt), []byte(body)})
	}
	if err := rows.Err(); err != nil {
		return nil, Manifest{}, err
	}
	if err := add("config.json", o.Config); err != nil {
		return nil, Manifest{}, err
	}

	red := NewRedactor(o.Secrets...)
	for i := range files {
		s, n := red.Redact(string(files[i].body))
		files[i].body = []byte(s)
		m.Redactions += n
		m.Files = append(m.Files, File{files[i].name, len(files[i].body)})
	}
	for i, note := range m.Notes {
		m.Notes[i], _ = red.Redact(note)
	}
	mb, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, Manifest{}, err
	}
	files = append([]file{{"manifest.json", append(mb, '\n')}}, files...)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, f := range files {
		hdr := &tar.Header{Name: f.name, Mode: 0o644, Size: int64(len(f.body)), ModTime: m.CreatedAt, Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, Manifest{}, err
		}
		if _, err := tw.Write(f.body); err != nil {
			return nil, Manifest{}, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, Manifest{}, err
	}
	if err := gz.Close(); err != nil {
		return nil, Manifest{}, err
	}
	return buf.Bytes(), m, nil
}

// CaseFor rebuilds the kernel Case behind a run's LAST claim: the events after the
// last run_started. An earlier claim was cut short by a lost lease; the last one
// re-ran everything from time 0 and is the one that finished.
func CaseFor(spec store.RunSpec, evs []events.Event) (pipeline.Case, error) {
	start := 0
	for i, e := range evs {
		if e.Type == "run_started" {
			start = i + 1
		}
	}
	return pipeline.CaseFromEvents(sched.KernelSteps(spec), spec.Concurrency, KernelEvents(evs[start:]))
}

// KernelEvents drops what the kernel does not emit (run_started) and the storage fields.
func KernelEvents(evs []events.Event) []pipeline.Event {
	out := []pipeline.Event{}
	for _, e := range evs {
		if e.Type == "run_started" {
			continue
		}
		k := pipeline.Event{AtMs: e.AtMs, Type: e.Type, Attempt: e.Attempt}
		if e.Step != nil {
			k.Step = *e.Step
		}
		if e.Reason != nil {
			k.Reason = *e.Reason
		}
		out = append(out, k)
	}
	return out
}
