// Package migrations holds the numbered SQL files graftd applies at startup.
// Never edit a file that has been applied anywhere: add a new one.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
