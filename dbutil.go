package dbdiff

import (
	"database/sql"
	"errors"
)

// Get all table name from db
func GetAllTables(db DbHolder, config *Configuration) ([]string, error) {
	var err error
	var tableNames []string
	var rows *sql.Rows
	switch config.Db.DbType {
	case "postgresql":
		rows, err = db.Query("select relname as TABLE_NAME from pg_stat_user_tables ORDER BY TABLE_NAME")
	case "mysql":
		rows, err = db.Query("SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema=database() ORDER BY TABLE_NAME")
	case "mssql":
		rows, err = db.Query("SELECT name AS TABLENAME FROM sys.objects WHERE type = 'U' ORDER BY TABLENAME")
	default:
		rows = nil
		err = errors.New("unsupported dbtype [" + config.Db.DbType + "]")
	}
	if err != nil {
		return []string{}, err
	}

	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return []string{}, err
		}
		tableNames = append(tableNames, tableName)
	}
	rows.Close()
	return tableNames, nil
}

// Get Primary key information
func GetPksOfTables(db DbHolder, config *Configuration, tableNames []string) (map[string][]string, error) {
	var err error
	schema := config.Db.Schema

	var stmt *sql.Stmt
	switch config.Db.DbType {
	case "postgresql":
		stmt, err = db.Prepare(`
		SELECT
		       kcu.ordinal_position AS PkOrder,
		       ccu.column_name AS ColumnName
		FROM
		     information_schema.table_constraints tb_con
		       INNER JOIN information_schema.constraint_column_usage ccu
		         ON tb_con.constraint_catalog = ccu.constraint_catalog
		              AND tb_con.constraint_schema = ccu.constraint_schema
		              AND tb_con.constraint_name = ccu.constraint_name
		       INNER JOIN information_schema.key_column_usage kcu
		         ON tb_con.constraint_catalog = kcu.constraint_catalog
		              AND tb_con.constraint_schema = kcu.constraint_schema
		              AND tb_con.constraint_name = kcu.constraint_name
		              AND ccu.column_name = kcu.column_name
		WHERE
		     tb_con.table_name = $1
		  AND tb_con.constraint_type = 'PRIMARY KEY'
		ORDER BY
		         tb_con.table_catalog
		    , tb_con.table_name
		    , tb_con.constraint_name
		    , kcu.ordinal_position
		`)
	case "mysql":
		stmt, err = db.Prepare(`
		SELECT
			ORDINAL_POSITION, COLUMN_NAME
		FROM
			information_schema.columns
		WHERE
			table_schema=database()
			AND TABLE_NAME = ?
			AND COLUMN_KEY = 'PRI'
		ORDER BY
			ORDINAL_POSITION
		`)
	case "mssql":
		stmt, err = db.Prepare(`
		SELECT
		       kcu.ordinal_position AS PkOrder,
		       ccu.column_name AS ColumnName
		FROM
		     information_schema.table_constraints tb_con
		       INNER JOIN information_schema.constraint_column_usage ccu
		         ON tb_con.constraint_catalog = ccu.constraint_catalog
		              AND tb_con.constraint_schema = ccu.constraint_schema
		              AND tb_con.constraint_name = ccu.constraint_name
		       INNER JOIN information_schema.key_column_usage kcu
		         ON tb_con.constraint_catalog = kcu.constraint_catalog
		              AND tb_con.constraint_schema = kcu.constraint_schema
		              AND tb_con.constraint_name = kcu.constraint_name
		              AND ccu.column_name = kcu.column_name
		WHERE
		     tb_con.table_name = @p1
		  AND tb_con.constraint_type = 'PRIMARY KEY'
		ORDER BY
		         tb_con.table_catalog
		    , tb_con.table_name
		    , tb_con.constraint_name
		    , kcu.ordinal_position
		`)
	}
	if err != nil || stmt == nil {
		return nil, err
	}
	defer stmt.Close()

	// PK取得(ORDER BYに使う)
	tablePks := make(map[string][]string, len(tableNames))
	for _, tableName := range tableNames {
		rows, err := stmt.Query(tableName)
		if err != nil {
			return nil, err
		}

		var columns []string
		for rows.Next() {
			var order int
			var columnName string
			err = rows.Scan(&order, &columnName)
			if err != nil {
				return nil, err
			}
			columns = append(columns, columnName)
		}
		rows.Close()

		if len(columns) == 0 {
			columns, err = GetAllColumnsOnTable(db, tableName, schema)
			if err != nil {
				return nil, err
			}
		}
		tablePks[tableName] = columns
	}

	return tablePks, nil
}

func GetAllColumnsOnTable(db DbHolder, tableName string, schema string) ([]string, error) {
	var columns []string

	// Pkがないので、対象テーブルの全カラム情報を取得して全カラムPKとみなす
	astQuery, err := db.Query("SELECT * FROM " + schema + tableName + " LIMIT 1")
	if err != nil {
		return nil, err
	}
	defer astQuery.Close()

	for astQuery.Next() {
		cols, err := astQuery.Columns()
		if err != nil {
			return nil, err
		}
		columns = append(columns, cols...)
	}
	return columns, nil
}
