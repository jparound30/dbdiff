package dbdiff

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"
)

type DBManager struct {
	db     *sql.DB
	closed bool
}

type DbHolder interface {
	Finalize() error

	Close() error

	// Wrapper for sql.DB#PingContext()
	PingContext(ctx context.Context) error

	// Wrapper for sql.DB#Ping()
	Ping() error

	// Wrapper for sql.DB#SetMaxIdleConns()
	SetMaxIdleConns(n int)

	// Wrapper for sql.DB#SetMaxOpenConns()
	SetMaxOpenConns(n int)

	// Wrapper for sql.DB#SetConnMaxLifetime()
	SetConnMaxLifetime(d time.Duration)

	// Wrapper for sql.DB#Stats()
	Stats() sql.DBStats

	// Wrapper for sql.DB#PrepareContext()
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)

	// Wrapper for sql.DB#Prepare()
	Prepare(query string) (*sql.Stmt, error)

	// Wrapper for sql.DB#ExecContext()
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	// Wrapper for sql.DB#Exec()
	Exec(query string, args ...interface{}) (sql.Result, error)

	// Wrapper for sql.DB#QueryContext()
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	// Wrapper for sql.DB#Query()
	Query(query string, args ...interface{}) (*sql.Rows, error)

	// Wrapper for sql.DB#QueryRowContext()
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row

	// Wrapper for sql.DB#QueryRow()
	QueryRow(query string, args ...interface{}) *sql.Row

	// Wrapper for sql.DB#BeginTx()
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)

	// Wrapper for sql.DB#Begin()
	Begin() (*sql.Tx, error)

	// Wrapper for sql.DB#Driver()
	Driver() driver.Driver

	// Wrapper for sql.DB#Conn()
	Conn(ctx context.Context) (*sql.Conn, error)
}
