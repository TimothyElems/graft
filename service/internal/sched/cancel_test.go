package sched

import (
	"context"
	"os"
	"testing"
	"time"

	"graft/kernel/pipeline"
	"graft/service/internal/runner"
	"graft/service/internal/store"
)

// With real containers: cancel a run whose steps are mid-sleep, and require that by
// the time Run returns, Docker has no container left for the run.
func TestCancelLeavesNoContainer(t *testing.T) {
	if os.Getenv("GRAFT_DOCKER") != "1" {
		t.Skip("set GRAFT_DOCKER=1 to run the Docker tests")
	}
	d := runner.Docker{}
	if c := os.Getenv("DOCKER_CONTEXT"); c != "" {
		d.Env = []string{"DOCKER_CONTEXT=" + c}
	}
	spec := store.RunSpec{Concurrency: 2, Steps: []store.StepSpec{
		{ID: "migrate", MaxAttempts: 1, Command: []string{"sleep", "60"}},
		{ID: "deploy", MaxAttempts: 1, Command: []string{"sleep", "60"}},
		{ID: "seed", Needs: []string{"migrate"}, MaxAttempts: 1, Command: []string{"sleep", "60"}},
	}}
	const runID = "run_cancel_docker"
	cancel := make(chan struct{})
	var seen []pipeline.Event
	go func() {
		// Wait until both containers really exist, then cancel.
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if ids, _ := d.Containers(context.Background(), runID); len(ids) == 2 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		close(cancel)
	}()
	res, _, err := Clock{Runner: d}.Run(context.Background(), runID, spec, cancel,
		func(evs []pipeline.Event) error { seen = append(seen, evs...); return nil })
	if err != nil {
		t.Fatal(err)
	}
	ids, err := d.Containers(context.Background(), runID)
	if err != nil || len(ids) != 0 {
		t.Fatalf("containers still present when Run returned: %v %v", ids, err)
	}
	if res.RunStatus != "cancelled" {
		t.Fatalf("run status %s", res.RunStatus)
	}
	for id, s := range res.Steps {
		if s.Status != "cancelled" {
			t.Errorf("step %s: %s, want cancelled", id, s.Status)
		}
	}
	t.Logf("cancelled at %d ms; containers left: 0; events: %d", res.FinishedMs, len(res.Events))
}
