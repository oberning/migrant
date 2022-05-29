package main

import (
	"github.com/oberning/migrant"
)

func main() {
	conf := migrant.Config{
		ConnectionUrl: "postgres://postgres:postgres@localhost:5432/migration_test",
		FileLocation:  "./cmd/example/testdata",
	}
	conf.Migrate()
}
