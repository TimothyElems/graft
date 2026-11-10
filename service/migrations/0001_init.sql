-- Graft state. Postgres 15+. One database; the queue is a table, not a broker.

CREATE TABLE projects (
  slug        text PRIMARY KEY CHECK (slug ~ '^[a-z][a-z0-9-]{1,40}$'),
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE branches (
  id          text PRIMARY KEY,
  project     text NOT NULL REFERENCES projects(slug),
  name        text NOT NULL,
  status      text NOT NULL CHECK (status IN ('creating','ready','failed','deleting','deleted')),
  git_ref     text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  expires_at  timestamptz NOT NULL
);
-- Names are unique among LIVE branches only, so a deleted branch's name can be reused (notebook case 06).
CREATE UNIQUE INDEX branches_live_name ON branches (project, name) WHERE status NOT IN ('deleting','deleted');
-- Cursor pages: newest first, ties broken by id (notebook case 07).
CREATE INDEX branches_project_created ON branches (project, created_at DESC, id DESC);
CREATE INDEX branches_expiry ON branches (expires_at) WHERE status = 'ready';

CREATE TABLE runs (
  id               text PRIMARY KEY,
  branch_id        text NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
  status           text NOT NULL CHECK (status IN ('queued','running','succeeded','failed','cancelled')),
  spec             jsonb NOT NULL,
  cancel_requested boolean NOT NULL DEFAULT false,
  created_at       timestamptz NOT NULL DEFAULT now(),
  finished_at      timestamptz,
  -- lease: a worker owns the run until lease_until. A crashed worker's lease lapses and another claims it.
  lease_owner      text,
  lease_until      timestamptz,
  claims           integer NOT NULL DEFAULT 0,   -- how many times a worker has claimed it
  -- per-run event counter. Bumped in the same transaction that inserts events, so seq has no gaps.
  event_seq        integer NOT NULL DEFAULT 0,
  error            text
);
-- The claim query: FOR UPDATE SKIP LOCKED over this partial index.
CREATE INDEX runs_claimable ON runs (created_at) WHERE status IN ('queued','running');
CREATE INDEX runs_branch ON runs (branch_id, created_at);

CREATE TABLE step_runs (
  run_id       text NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_id      text NOT NULL,
  position     integer NOT NULL,
  status       text NOT NULL CHECK (status IN ('pending','succeeded','failed','skipped','cancelled')),
  attempts     integer NOT NULL DEFAULT 0,
  started_ms   bigint,
  finished_ms  bigint,
  reason       text,
  PRIMARY KEY (run_id, step_id)
);

CREATE TABLE events (
  id         text PRIMARY KEY,
  run_id     text NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  branch_id  text NOT NULL REFERENCES branches(id) ON DELETE CASCADE,
  seq        integer NOT NULL CHECK (seq >= 1),
  type       text NOT NULL,
  at_ms      bigint NOT NULL,
  step       text,
  attempt    integer NOT NULL DEFAULT 0,
  reason     text,
  at         timestamptz NOT NULL DEFAULT now(),
  UNIQUE (run_id, seq)
);
CREATE INDEX events_branch_at ON events (branch_id, at, id);

-- Idempotency-Key: the first response for (scope, key), and a hash of the request that produced it.
-- Same key + same request -> replay. Same key + different request -> 422.
CREATE TABLE idempotency_keys (
  scope         text NOT NULL,             -- method and path, e.g. "POST /v1/branches/br_x/runs"
  key           text NOT NULL CHECK (length(key) BETWEEN 1 AND 64),
  request_hash  bytea NOT NULL,
  status_code   integer NOT NULL,
  response      bytea NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (scope, key)
);
CREATE INDEX idempotency_keys_created ON idempotency_keys (created_at);
