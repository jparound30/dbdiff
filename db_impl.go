package dbdiff

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"sync"
	"time"
)
import _ "github.com/jackc/pgx/stdlib"
import _ "github.com/go-sql-driver/mysql"

var holder = &DBManager{db: nil, closed: true}
var lock sync.Mutex

func GetDBInstance(dbConfig *Db) (*DBManager, error) {

	var err error

	lock.Lock()
	if holder.closed {
		// TODO Postgres以外対応するならこの辺りは見直し必要
		var connStr string
		var driverName string
		if dbConfig.DbType == "postgresql" {
			const connStringTemplate = "postgresql://%s:%s@%s:%s/%s"
			fmt.Printf("Connect to ... "+connStringTemplate+"\n", dbConfig.User, "********", dbConfig.Host, dbConfig.Port, dbConfig.Name)
			connStr = fmt.Sprintf(connStringTemplate, dbConfig.User, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Name)
			driverName = "pgx"
		} else if dbConfig.DbType == "mysql" {
			//connStringMysql = "username:password@protocol(address)/dbname"
			const connStringMysql = "%s:%s@tcp(%s:%s)/%s"
			fmt.Printf("Connect to ... "+connStringMysql+"\n", dbConfig.User, "********", dbConfig.Host, dbConfig.Port, dbConfig.Name)
			connStr = fmt.Sprintf(connStringMysql, dbConfig.User, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Name)
			driverName = "mysql"
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

// PingContext verifies a connection to the database is still alive,
// establishing a connection if necessary.
func (holder *DBManager) PingContext(ctx context.Context) error {
	return holder.db.PingContext(ctx)
}

// Ping verifies a connection to the database is still alive,
// establishing a connection if necessary.
func (holder *DBManager) Ping() error {
	return holder.db.PingContext(context.Background())
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
//
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns,
// then the new MaxIdleConns will be reduced to match the MaxOpenConns limit.
//
// If n <= 0, no idle connections are retained.
//
// The default max idle connections is currently 2. This may change in
// a future release.
func (holder *DBManager) SetMaxIdleConns(n int) {
	holder.db.SetMaxIdleConns(n)
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
//
// If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than
// MaxIdleConns, then MaxIdleConns will be reduced to match the new
// MaxOpenConns limit.
//
// If n <= 0, then there is no limit on the number of open connections.
// The default is 0 (unlimited).
func (holder *DBManager) SetMaxOpenConns(n int) {
	holder.db.SetMaxOpenConns(n)
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are reused forever.
func (holder *DBManager) SetConnMaxLifetime(d time.Duration) {
	holder.db.SetConnMaxLifetime(d)
}

// Stats returns database statistics.
func (holder *DBManager) Stats() sql.DBStats {
	return holder.db.Stats()
}

// PrepareContext creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
//
// The provided context is used for the preparation of the statement, not for the
// execution of the statement.
func (holder *DBManager) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return holder.db.PrepareContext(ctx, query)
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
func (holder *DBManager) Prepare(query string) (*sql.Stmt, error) {
	return holder.db.PrepareContext(context.Background(), query)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (holder *DBManager) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return holder.db.ExecContext(ctx, query, args)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (holder *DBManager) Exec(query string, args ...interface{}) (sql.Result, error) {
	return holder.db.ExecContext(context.Background(), query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (holder *DBManager) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return holder.db.QueryContext(ctx, query, args)
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (holder *DBManager) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return holder.db.QueryContext(context.Background(), query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
func (holder *DBManager) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return holder.db.QueryRowContext(ctx, query, args)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
func (holder *DBManager) QueryRow(query string, args ...interface{}) *sql.Row {
	return holder.db.QueryRowContext(context.Background(), query, args...)
}

// BeginTx starts a transaction.
//
// The provided context is used until the transaction is committed or rolled back.
// If the context is canceled, the sql package will roll back
// the transaction. Tx.Commit will return an error if the context provided to
// BeginTx is canceled.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (holder *DBManager) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return holder.db.BeginTx(ctx, opts)
}

// Begin starts a transaction. The default isolation level is dependent on
// the driver.
func (holder *DBManager) Begin() (*sql.Tx, error) {
	return holder.db.BeginTx(context.Background(), nil)
}

// Driver returns the database's underlying driver.
func (holder *DBManager) Driver() driver.Driver {
	return holder.db.Driver()
}

// Conn returns a single connection by either opening a new connection
// or returning an existing connection from the connection pool. Conn will
// block until either a connection is returned or ctx is canceled.
// Queries run on the same Conn will be run in the same database session.
//
// Every Conn must be returned to the database pool after use by
// calling Conn.Close.
func (holder *DBManager) Conn(ctx context.Context) (*sql.Conn, error) {
	return holder.db.Conn(ctx)
}
