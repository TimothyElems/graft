.PHONY: test test-go test-py diff contract all test-service test-sdk e2e e2e-compose fmt-check pg-dev
export PATH := $(PATH):/usr/local/go/bin

all: contract test diff test-service

contract:
	python3 -c "import yaml,pathlib; from openapi_spec_validator import validate; p=pathlib.Path('contract/openapi.yaml').resolve(); validate(yaml.safe_load(p.read_text()), base_uri=p.as_uri())"
	python3 -c "import json,jsonschema; jsonschema.Draft202012Validator.check_schema(json.load(open('contract/events.schema.json')))"
	python3 -c "import pglast; pglast.parse_sql(open('db/schema.sql').read())"

test-go:
	cd kernel && go vet ./... && go test -race ./...

test-py:
	python3 -m pytest parity -q

test: test-go test-py

diff:
	python3 parity/diff_random.py 300 1

fmt-check:        ## same gate as CI: fail if any Go file needs gofmt
	@out=$$(gofmt -l service kernel); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

test-service: fmt-check   ## needs GRAFT_TEST_DATABASE_URL (see `make pg-dev`)
	@if [ -z "$$GRAFT_TEST_DATABASE_URL" ]; then echo "GRAFT_TEST_DATABASE_URL is not set. Start one with: make pg-dev"; exit 1; fi
	cd service && go vet ./... && go test -race -count=1 ./...

pg-dev:           ## start a throwaway local Postgres and print the env var to export
	@./scripts/pg-dev.sh start

test-sdk:
	cd sdk/python && .venv/bin/python -m pytest tests/unit -q

e2e:              ## graftd binary + SDK e2e tests, no Docker needed
	./scripts/e2e-local.sh

e2e-compose:      ## the same tests against `docker compose up` (what CI runs)
	docker compose up --build --detach --wait
	cd sdk/python && GRAFT_E2E=1 GRAFT_TOKEN=local-dev-token-change-me .venv/bin/python -m pytest tests/e2e -v; s=$$?; cd ../.. && docker compose down; exit $$s
