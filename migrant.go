package migrant

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v4"
	"github.com/juju/naturalsort"
)

const debug = false

type Config struct {
	ConnectionUrl string
	FileLocation  string
	TableName     string
	Files         []File
}

type File struct {
	FileName string
	Checksum string
}

func (conf *Config) Migrate() {
	log.Println("INFO: Migration started.")
	ctx := context.Background()
	conf.checkOrSetDefaultConfig()
	conn, err := pgx.Connect(ctx, conf.ConnectionUrl)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	err = conf.getSqlFilesProperties()
	if err != nil {
		return
	}
	err = conf.createTableIfNotExists(&ctx, conn)
	if err != nil {
		return
	}
	conf.compareOrExecuteSql(&ctx, conn)
	log.Println("INFO: Migration done.")
}

func (conf *Config) getSqlFilesProperties() error {
	path := conf.FileLocation
	dirEntries, err := os.ReadDir(path)
	if err != nil {
		log.Println(err)
		return err
	}
	var filenames []string
	for _, f := range dirEntries {
		filenames = append(filenames, f.Name())
	}
	sortedFilenames := naturalsort.Sort(filenames)
	var fcs []File
	for _, f := range sortedFilenames {
		fc := File{
			FileName: f,
			Checksum: createMd5Sum(filepath.Join(path, f)),
		}
		fcs = append(fcs, fc)
	}
	conf.Files = fcs
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

func (conf *Config) createTableIfNotExists(ctx *context.Context, conn *pgx.Conn) error {
	tx, err := conn.Begin(*ctx)
	if err != nil {
		log.Println(err)
		return err
	}
	defer tx.Rollback(*ctx)

	sql := fmt.Sprintf(`create table if not exists %s (
	version_id serial primary key,
	filename varchar not null,
	checksum varchar not null
	);`, conf.TableName)
	_, err = tx.Exec(*ctx, sql)
	if err != nil {
		log.Println(err)
		return err
	}

	sql = fmt.Sprintf(`create unique index if not exists %s_filename on %s (
		filename asc
	)
	`, conf.TableName, conf.TableName)
	_, err = tx.Exec(*ctx, sql)
	if err != nil {
		log.Println(err)
		return err
	}

	err = tx.Commit(*ctx)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (conf *Config) compareOrExecuteSql(ctx *context.Context, conn *pgx.Conn) error {
	var (
		versionId int
		checksum  string
	)
	selectStmt := fmt.Sprintf("select version_id, checksum from %s where filename = $1", conf.TableName)
	for _, file := range conf.Files {
		err := conn.QueryRow(*ctx, selectStmt, file.FileName).Scan(&versionId, &checksum)
		if err != nil {
			if err.Error() == "no rows in result set" { // Migration was not executed before
				// Execute migration
				if err := conf.executeSqlFile(ctx, conn, file); err != nil {
					return err
				}
				// Insert checksum into the migration table
				if err := conf.insertIntoDbMigration(ctx, conn, file); err != nil {
					return err
				}
				log.Printf("INFO: Migration successful: file %s, checksum %s\n", file.FileName, file.Checksum)
			} else {
				log.Println(err)
				return err
			}
			continue
		}
		if file.Checksum != checksum { // Mismatch in checksums
			err = errors.New("checksums do not match")
			log.Fatalf("ERROR: Checksum of file %s is %s but is expected to be %s. Error: %s\n",
				file.FileName, file.Checksum, checksum, err)
		}
		if debug {
			log.Printf("DEBUG: Skipped existing migration: Version %d, checksum %s\n", versionId, checksum)
		}
	}
	return nil
}

func (conf *Config) insertIntoDbMigration(ctx *context.Context, conn *pgx.Conn, file File) error {
	tx, err := conn.Begin(*ctx)
	if err != nil {
		log.Println(err)
		return err
	}
	defer tx.Rollback(*ctx)

	stmt := fmt.Sprintf("insert into %s (filename, checksum) values ($1, $2);", conf.TableName)
	_, err = tx.Exec(*ctx, stmt, file.FileName, file.Checksum)
	if err != nil {
		log.Fatalf("ERROR: Could not insert DB record into %s. Check if the migration file %s has been executed successfully. Error: %s\n",
			conf.TableName, file.FileName, err)
	}

	err = tx.Commit(*ctx)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (conf *Config) executeSqlFile(ctx *context.Context, conn *pgx.Conn, file File) error {
	tx, err := conn.Begin(*ctx)
	if err != nil {
		log.Println(err)
		return err
	}
	defer tx.Rollback(*ctx)

	pathToFile := filepath.Join(conf.FileLocation, file.FileName)
	sql, err := os.ReadFile(pathToFile)
	if err != nil {
		log.Printf("ERROR: Unable to read %s\n", pathToFile)
		return err
	}

	_, err = tx.Exec(*ctx, string(sql))
	if err != nil {
		log.Fatalf("ERROR: Could not execute SQL file %s. Error: %s\n", pathToFile, err)
	}

	err = tx.Commit(*ctx)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func createMd5Sum(filename string) string {
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
