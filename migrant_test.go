package migrant

import (
	"testing"
)

func TestMigrationFiles(t *testing.T) {
	conf := Config{
		ConnectionUrl: "postgres://postgres:postgres@localhost:5432/migration_test",
		FileLocation:  "./cmd/example/testdata",
	}

	expected := []struct {
		Filename string
		Checksum string
	}{
		{Filename: "v1_create_test_table1.sql", Checksum: "78c1f763b18db3d75a4321a08e49db40"},
		{Filename: "v2_create_test_table3.sql", Checksum: "d41d8cd98f00b204e9800998ecf8427e"},
		{Filename: "v10_create_test_table2.sql", Checksum: "d41d8cd98f00b204e9800998ecf8427e"},
		{Filename: "v11_alter_table1.sql", Checksum: "d41d8cd98f00b204e9800998ecf8427e"},
	}
	err := conf.getSqlFilesProperties()
	if err != nil {
		t.Error(err)
	}
	for i, f := range conf.Files {
		if expected[i].Filename != f.FileName || expected[i].Checksum != f.Checksum {
			t.Errorf("Expected %v, but got %v", expected[i], f)
		}
	}
}
