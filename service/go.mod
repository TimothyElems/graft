module graft/service

go 1.24.0

toolchain go1.24.5

require (
	github.com/coder/websocket v1.8.15
	github.com/jackc/pgx/v5 v5.8.0
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2
	graft/kernel v0.0.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace graft/kernel => ../kernel
