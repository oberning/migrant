# Migrant
Small database migration tool for PostgreSQL similar to Flyway for Java/Kotlin.

## How to use
```go
package main

import (
	"github.com/oberning/migrant"
)

func main() {
	db := migrant.Config{ // (1)
		Url:          "postgres://postgres:postgres@localhost:5432/migration_test", // (2)
		FileLocation: "./cmd/example/testdata", // (3)
	}
	db.Migrate() // (4)
}
```

1. Define the `Config` struct
2. Connection string for the PostgreSQL database. The library `github.com/jackc/pgx/v4` is used.
3. The folder that contains the migration files. Because the SQL files are read in the order as per naturally sorted filenames. See `./cmd/example/testdata` for an example. The files in that folder are read in that order: `v1_*`, `v2_*`, `v10_*`, `v11_*`.
4. That line finally executes the migration.
