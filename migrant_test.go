package migrant

import (
	"errors"
	"path/filepath"
	"testing"
)

type fakeFileSystem struct {
	Files []File
}

func (fs *fakeFileSystem) readDirectory(path string) []string {
	return []string{"file_1", "file_2", "file_3"}
}

func (fs *fakeFileSystem) addToFileList(files []File) {
	fs.Files = files
}

func (fs *fakeFileSystem) createMd5Sum(filename string) string {
	switch filename {
	case filepath.Join("dir", "file_1"):
		return "checksum_1"
	case filepath.Join("dir", "file_2"):
		return "checksum_2"
	case filepath.Join("dir", "file_3"):
		return "checksum_3"
	}
	return "checksum_invalid"
}

type fakeDbConnect struct {
	executed     string
	inserted     string
	isCommitted  bool
	isRollbacked bool
	isExecuted   bool
	isOpened     bool
	isClosed     bool
}

func (db *fakeDbConnect) query(filename string, tablename string) (table, error) {
	var tbl table
	switch filename {
	case "file_1":
		tbl = table{
			versionId: 1,
			checksum:  "checksum_1",
		}
		return tbl, nil
	case "file_2":
		tbl = table{
			versionId: 1,
			checksum:  "checksum_2",
		}
		return tbl, nil
	case "file_3":
		tbl = table{
			versionId: 1,
			checksum:  "checksum_3",
		}
		return tbl, nil
	case "file_5":
		tbl = table{
			versionId: 1,
			checksum:  "checksum_5",
		}
		return tbl, nil
	}
	return table{}, errors.New("no rows in result set")
}

func (db *fakeDbConnect) executeSqlFile(path string) error {
	db.executed = "file_4"
	return nil
}

func (db *fakeDbConnect) insertIntoDbMigration(tablename string, file File) error {
	db.inserted = "file_4"
	return nil
}

func (db *fakeDbConnect) begin() error {
	return nil
}

func (db *fakeDbConnect) rollback() {
}

func (db *fakeDbConnect) exec(str string) error {
	return nil
}

func (db *fakeDbConnect) commit() error {
	return nil
}

func (db *fakeDbConnect) connect(str string) error {
	return nil
}

func (db *fakeDbConnect) close() {
}

func TestGetSqlFilesProperties(t *testing.T) {
	wanted := []struct {
		filename string
		checksum string
	}{
		{"file_1", "checksum_1"},
		{"file_2", "checksum_2"},
		{"file_3", "checksum_3"},
	}
	fs := fakeFileSystem{}
	err := getSqlFilesProperties(&fs, "dir")
	if err != nil {
		t.Error(err)
	}
	testFiles := [3]string{"file_1", "file_2", "file_3"}
	for i, _ := range fs.Files {
		if fs.Files[i].FileName != testFiles[i] {
			t.Errorf("Expected %s in the sorted list but got %s", testFiles[i], fs.Files[i].FileName)
		}
	}

	actual := make(map[string]string)
	for _, i := range fs.Files {
		actual[i.FileName] = i.Checksum
	}
	for _, tt := range wanted {
		if tt.checksum != actual[tt.filename] {
			t.Errorf("Expected %s for file %s but got %s", actual[tt.filename], tt.filename, tt.checksum)
		}
	}
}

func TestCompareOrExecuteSql(t *testing.T) {
	// Test Case 1
	files := []File{
		{"file_1", "checksum_1"},
		{"file_2", "checksum_2"},
		{"file_3", "checksum_3"},
		{"file_4", "checksum_4"},
	}
	db := fakeDbConnect{}
	err := compareOrExecuteSql(&db, "dir", "testtable", files)
	if err != nil {
		t.Error("An unexpected error occurred!")
	}
	if db.executed != "file_4" {
		t.Error("File_4 not executed")
	}

	if db.inserted != "file_4" {
		t.Error("File_4 not inserted")
	}
	// Test Case 2 - Checksum Mismatch
	newfiles := []File{
		{"file_5", "checksum_wrong"},
	}
	err = compareOrExecuteSql(&db, "dir", "testtable", newfiles)
	if err != nil {
		if err.Error() == "checksums do not match" {
			t.Log("OK: The checksum error is expected.")
		} else {
			t.Error("In this test case there is a checksum mismatch and that should be the only error!")
		}
	}
}
