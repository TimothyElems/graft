-- v0.7: keep what each attempt printed, so a support bundle can include it.
-- One row per attempt per claim: a restarted run (claim 2) runs its steps again, and
-- both runs' output is evidence. Only the last 8 KB is kept (runner.OutputTail).
-- Written fenced on the lease, like events: a worker that lost its lease writes nothing.
CREATE TABLE attempt_logs (
  run_id      text NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  claim       integer NOT NULL CHECK (claim >= 1),
  step_id     text NOT NULL,
  attempt     integer NOT NULL CHECK (attempt >= 1),
  outcome     text NOT NULL CHECK (outcome IN ('ok','fail','timeout','cancelled')),
  exit_code   integer NOT NULL,
  took_ms     bigint NOT NULL,
  output      text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, claim, step_id, attempt)
);
