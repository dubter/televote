// Package migrations хранит схему Postgres.
package migrations

import "embed"

// FS — все SQL-миграции проекта.
//
//go:embed *.sql
var FS embed.FS
