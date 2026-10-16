// Package migrations embeds the reviewed schema files into the migration executable.
package migrations

import "embed"

// Files contains immutable versioned SQL; runtime execution never depends on the working directory.
//
//go:embed *.sql
var Files embed.FS
