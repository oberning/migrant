package main

import (
	"github.com/oberning/migrant"
)

func main() {
	db := migrant.Config{
		Url:          "postgres://postgres:postgres@localhost:5432/migration_test",
		FileLocation: "./cmd/example/testdata",
	}
	db.Migrate()
}
