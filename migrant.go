package migrant

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"

	//"errors"
	//"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v4"
	"github.com/juju/naturalsort"
)

const debug = false

type File struct {
	FileName string
	Checksum string
}

type fileSystem struct {
	Files []File
}

type fileSystemer interface {
	readDirectory(string) []string
	createMd5Sum(string) string
	addToFileList([]File)
}

type Config struct {
	FileLocation string
	Url          string
	TableName    string
}

type DbConnection struct {
	ctx  context.Context
	conn *pgx.Conn
	tx   pgx.Tx
}

type dbconnector interface {
	connect(string) error
	close()
}

type dbtransactor interface {
	begin() error
	rollback()
	exec(string) error
	commit() error
}

type dbfuncer interface {
	query(string, string) (table, error)
	executeSqlFile(string) error
	insertIntoDbMigration(string, File) error
}

type databaser interface {
	dbconnector
	dbtransactor
	dbfuncer
}

type table struct {
	versionId int
	checksum  string
}

func (conf *Config) Migrate() {
	log.Println("INFO: Migration started.")
	var dbconn DbConnection
	dbconn.ctx = context.Background()

	conf.checkOrSetDefaultConfig()
	var err error
	err = dbconn.connect(conf.Url)
	if err != nil {
		log.Fatal(err)
	}
	defer dbconn.close()

	fs := fileSystem{}
	err = getSqlFilesProperties(&fs, conf.FileLocation)
	if err != nil {
		return
	}
	err = createTableIfNotExists(&dbconn, conf.TableName)
	if err != nil {
		return
	}
	compareOrExecuteSql(&dbconn, conf.FileLocation, conf.TableName, fs.Files)
	log.Println("INFO: Migration done.")
}

func (dbconn *DbConnection) connect(url string) error {
	conn, err := pgx.Connect(dbconn.ctx, url)
	dbconn.conn = conn
	return err
}

func (dbconn *DbConnection) close() {
	dbconn.conn.Close(dbconn.ctx)
}

func (dbconn *DbConnection) begin() error {
	var tx pgx.Tx
	tx, err := dbconn.conn.Begin(dbconn.ctx)
	dbconn.tx = tx
	return err
}

func (dbconn *DbConnection) rollback() {
	dbconn.tx.Rollback(dbconn.ctx)
}

func (dbconn *DbConnection) exec(sql string) error {
	_, err := dbconn.tx.Exec(dbconn.ctx, sql)
	return err
}

func (dbconn *DbConnection) commit() error {
	err := dbconn.tx.Commit(dbconn.ctx)
	return err
}

func (fs *fileSystem) readDirectory(path string) []string {
	dirEntries, err := os.ReadDir(path)
	if err != nil {
		log.Fatalln(err)
	}
	var filenames []string
	for _, f := range dirEntries {
		filenames = append(filenames, f.Name())
	}
	return filenames
}

func (fs *fileSystem) addToFileList(files []File) {
	fs.Files = files
}

func getSqlFilesProperties(fs fileSystemer, path string) error { // Write a test for this
	filenames := fs.readDirectory(path)
	sortedFilenames := naturalsort.Sort(filenames)
	var fcs []File
	for _, f := range sortedFilenames {
		fc := File{
			FileName: f,
			Checksum: fs.createMd5Sum(filepath.Join(path, f)),
		}
		fcs = append(fcs, fc)
	}
	fs.addToFileList(fcs)
	return nil
}

func (conf *Config) checkOrSetDefaultConfig() {
	if conf.TableName == "" {
		conf.TableName = "_db_migration"
	}
	if conf.FileLocation == "" {
		conf.FileLocation = "./assets/sql"
	}
}

func createTableIfNotExists(dbconn databaser, table string) error {
	err := dbconn.begin()
	if err != nil {
		log.Println(err)
		return err
	}
	defer dbconn.rollback()

	sql := fmt.Sprintf(`create table if not exists %s (
	version_id serial primary key,
	filename varchar not null,
	checksum varchar not null
	);`, table)
	err = dbconn.exec(sql)
	if err != nil {
		log.Println(err)
		return err
	}

	sql = fmt.Sprintf(`create unique index if not exists %s_filename on %s (
		filename asc
	)
	`, table, table)
	err = dbconn.exec(sql)
	if err != nil {
		log.Println(err)
		return err
	}

	err = dbconn.commit()
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (dbconn *DbConnection) query(filename string, tablename string) (table, error) {
	table := table{}
	selectStmt := fmt.Sprintf("select version_id, checksum from %s where filename = $1", tablename)
	err := dbconn.conn.QueryRow(dbconn.ctx, selectStmt, filename).Scan(table.versionId, table.checksum)
	return table, err
}

func compareOrExecuteSql(dbconn databaser, path string, tablename string, files []File) error {
	var (
		tbl table
		err error
	)
	for _, file := range files {
		tbl, err = dbconn.query(file.FileName, tablename)
		if err != nil {
			if err.Error() == "no rows in result set" { // Migration was not executed before
				// Execute migration
				if err := dbconn.executeSqlFile(filepath.Join(path, file.FileName)); err != nil {
					return err
				}
				// Insert checksum into the migration table
				if err := dbconn.insertIntoDbMigration(tablename, file); err != nil {
					return err
				}
				log.Printf("INFO: Migration successful: file %s, checksum %s\n", file.FileName, file.Checksum)
			} else {
				log.Println(err)
				return err
			}
			continue
		}
		if file.Checksum != tbl.checksum { // Mismatch in checksums
			err = errors.New("checksums do not match")
			log.Printf("ERROR: Checksum of file %s is %s but is expected to be %s. Error: %s\n",
				file.FileName, file.Checksum, tbl.checksum, err)
			return err
		}
		if debug {
			log.Printf("DEBUG: Skipped existing migration: Version %d, checksum %s\n", tbl.versionId, tbl.checksum)
		}
	}
	return nil
}

func (dbconn *DbConnection) insertIntoDbMigration(tablename string, file File) error {
	tx, err := dbconn.conn.Begin(dbconn.ctx)
	if err != nil {
		log.Println(err)
		return err
	}
	defer tx.Rollback(dbconn.ctx)

	stmt := fmt.Sprintf("insert into %s (filename, checksum) values ($1, $2);", tablename)
	_, err = tx.Exec(dbconn.ctx, stmt, file.FileName, file.Checksum)
	if err != nil {
		log.Fatalf("ERROR: Could not insert DB record into %s. Check if the migration file %s has been executed successfully. Error: %s\n",
			tablename, file.FileName, err)
	}

	err = tx.Commit(dbconn.ctx)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (dbconn *DbConnection) executeSqlFile(path string) error {
	err := dbconn.begin()
	if err != nil {
		log.Println(err)
		return err
	}
	defer dbconn.rollback()

	sql, err := os.ReadFile(path)
	if err != nil {
		log.Printf("ERROR: Unable to read %s\n", path)
		return err
	}

	err = dbconn.exec(string(sql))
	if err != nil {
		log.Fatalf("ERROR: Could not execute SQL file %s. Error: %s\n", path, err)
	}

	err = dbconn.commit()
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (fs *fileSystem) createMd5Sum(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Println(err)
		return ""
	}
	defer file.Close()

	hash := md5.New()
	_, err = io.Copy(hash, file)
	if err != nil {
		log.Println(err)
		return ""
	}

	return hex.EncodeToString(hash.Sum(nil))
}
