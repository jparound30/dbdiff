package dbdiff

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import _ "github.com/jackc/pgx/stdlib"
import _ "github.com/go-sql-driver/mysql"
import _ "github.com/denisenkom/go-mssqldb"

var holder = &DBManager{db: nil, closed: true}
var lock sync.Mutex

func GetDBInstance(dbConfig *Db) (*DBManager, error) {

	var err error

	lock.Lock()
	if holder.closed {
		var connStr string
		var driverName string
		switch dbConfig.DbType {
		case "postgresql":
			const connStringPostgreSQL = "postgresql://%s:%s@%s:%s/%s"
			fmt.Printf("Connect to ... "+connStringPostgreSQL+"\n", dbConfig.User, "********", dbConfig.Host, dbConfig.Port, dbConfig.Name)
			connStr = fmt.Sprintf(connStringPostgreSQL, dbConfig.User, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Name)
			driverName = "pgx"
		case "mysql":
			// "username:password@protocol(address)/dbname"
			const connStringMysql = "%s:%s@tcp(%s:%s)/%s"
			fmt.Printf("Connect to ... "+connStringMysql+"\n", dbConfig.User, "********", dbConfig.Host, dbConfig.Port, dbConfig.Name)
			connStr = fmt.Sprintf(connStringMysql, dbConfig.User, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Name)
			driverName = "mysql"
		case "mssql":
			const connStringMSSql = "user id=%s;password=%s;server=%s;port=%s;database=%s;"
			fmt.Printf("Connect to ... "+connStringMSSql+"\n", dbConfig.User, "********", dbConfig.Host, dbConfig.Port, dbConfig.Name)
			connStr = fmt.Sprintf(connStringMSSql, dbConfig.User, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Name)
			driverName = "sqlserver"
		default:
			err := errors.New("unknown DbType")
			return nil, err
		}
		holder.db, err = sql.Open(driverName, connStr)
		if err != nil {
			log.Fatal("[DB] can not get DB", err)
		}
		// TODO avoid "unexpected EOF"...
		if dbConfig.DbType == "mysql" {
			holder.db.SetMaxIdleConns(0)
		}
	}
	lock.Unlock()

	return holder, err
}

func (holder *DBManager) Finalize() error {
	var err error
	lock.Lock()
	if !holder.closed {
		err = holder.db.Close()
		holder.closed = true
		if err != nil {
			log.Fatal("[DB] can not get DB", err)
		}
	}
	lock.Unlock()
	return err
}

func (holder *DBManager) Close() error {
	var err error
	fmt.Println("DBManager:Close()")
	return err
}

// Wrapper for sql.DB#PingContext()
func (holder *DBManager) PingContext(ctx context.Context) error {
	return holder.db.PingContext(ctx)
}

// Wrapper for sql.DB#Ping()
func (holder *DBManager) Ping() error {
	return holder.db.PingContext(context.Background())
}

// Wrapper for sql.DB#SetMaxIdleConns()
func (holder *DBManager) SetMaxIdleConns(n int) {
	holder.db.SetMaxIdleConns(n)
}

// Wrapper for sql.DB#SetMaxOpenConns()
func (holder *DBManager) SetMaxOpenConns(n int) {
	holder.db.SetMaxOpenConns(n)
}

// Wrapper for sql.DB#SetConnMaxLifetime()
func (holder *DBManager) SetConnMaxLifetime(d time.Duration) {
	holder.db.SetConnMaxLifetime(d)
}

// Wrapper for sql.DB#Stats()
func (holder *DBManager) Stats() sql.DBStats {
	return holder.db.Stats()
}

// Wrapper for sql.DB#PrepareContext()
func (holder *DBManager) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return holder.db.PrepareContext(ctx, query)
}

// Wrapper for sql.DB#Prepare()
func (holder *DBManager) Prepare(query string) (*sql.Stmt, error) {
	return holder.db.PrepareContext(context.Background(), query)
}

// Wrapper for sql.DB#ExecContext()
func (holder *DBManager) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return holder.db.ExecContext(ctx, query, args)
}

// Wrapper for sql.DB#Exec()
func (holder *DBManager) Exec(query string, args ...interface{}) (sql.Result, error) {
	return holder.db.ExecContext(context.Background(), query, args...)
}

// Wrapper for sql.DB#QueryContext()
func (holder *DBManager) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return holder.db.QueryContext(ctx, query, args)
}

// Wrapper for sql.DB#Query()
func (holder *DBManager) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return holder.db.QueryContext(context.Background(), query, args...)
}

// Wrapper for sql.DB#QueryRowContext()
func (holder *DBManager) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return holder.db.QueryRowContext(ctx, query, args)
}

// Wrapper for sql.DB#QueryRow()
func (holder *DBManager) QueryRow(query string, args ...interface{}) *sql.Row {
	return holder.db.QueryRowContext(context.Background(), query, args...)
}

// Wrapper for sql.DB#BeginTx()
func (holder *DBManager) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return holder.db.BeginTx(ctx, opts)
}

// Wrapper for sql.DB#Begin()
func (holder *DBManager) Begin() (*sql.Tx, error) {
	return holder.db.BeginTx(context.Background(), nil)
}

// Wrapper for sql.DB#Driver()
func (holder *DBManager) Driver() driver.Driver {
	return holder.db.Driver()
}

// Wrapper for sql.DB#Conn()
func (holder *DBManager) Conn(ctx context.Context) (*sql.Conn, error) {
	return holder.db.Conn(ctx)
}
