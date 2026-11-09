.PHONY: test test-go test-py diff contract all
export PATH := $(PATH):/usr/local/go/bin

all: contract test diff

contract:
	python3 -c "import yaml,pathlib; from openapi_spec_validator import validate; p=pathlib.Path('contract/openapi.yaml').resolve(); validate(yaml.safe_load(p.read_text()), base_uri=p.as_uri())"
	python3 -c "import json,jsonschema; jsonschema.Draft202012Validator.check_schema(json.load(open('contract/events.schema.json')))"
	python3 -c "import pglast; pglast.parse_sql(open('db/schema.sql').read())"

test-go:
	cd kernel && go vet ./... && go test ./...

test-py:
	python3 -m pytest parity -q

test: test-go test-py

diff:
	python3 parity/diff_random.py 300 1
