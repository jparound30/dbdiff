package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/jparound30/dbdiff"
	"log"
	_ "net/http/pprof"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func main() {
	// TODO 引数でconfig指定
	configuration, err := dbdiff.LoadConfiguration("")
	if err != nil {
		log.Fatal("Failed to load configuration file.")
	}
	db, err := dbdiff.GetDBInstance(&configuration.Db)
	if err != nil {
		log.Fatal("DB instance initialization failed.")
	}
	defer db.Finalize()
	if configuration.Db.DbType == "mysql" {
		db.SetMaxIdleConns(0)
	}

	fmt.Println("[INITIALIZING] Collecting Table Information ...")
	tableNames, err := getAllTables(db)
	checkErr(err)

	tablePks, err := getPksOfTables(db, tableNames)
	checkErr(err)

	//for key, value := range tablePks {
	//	fmt.Printf("TABLE:%s, PK_COLUMN:%s\n", key, value)
	//}

	fmt.Print("[BEFORE] Collecting snapshot data...")
	before, err := collectAllTableData(db, tablePks)
	checkErr(err)
	fmt.Printf(", Total record count: %d ...", before.TotalDataCount)
	fmt.Println(" COMPLETE!")

	fmt.Printf("OK, Let's do some operations, THEN HIT ANY KEY!")
	stdin := bufio.NewScanner(os.Stdin)
	stdin.Scan()

	fmt.Print("\n[AFTER ] Collecting snapshot data...")
	after, err := collectAllTableData(db, tablePks)
	checkErr(err)
	fmt.Printf(", Total record count: %d ...", after.TotalDataCount)
	fmt.Println("COMPLETE!")

	// TODO プロファイル用 そのうち削除
	//var wg sync.WaitGroup
	//
	//go func() {
	//	log.Println(http.ListenAndServe(":6060", nil))
	//}()
	//
	//wg.Add(1)
	//wg.Wait()

	// TODO Excel出力　要refactoring
	extractChangedData := after.extractChangedData(before)
	xlsx := excelize.NewFile()
	const SheetName = "Sheet1"
	xlsx.NewSheet(SheetName)
	var ri = 2
	var ci = 1
	modCellStyle, _ := xlsx.NewStyle(`{"fill":{"type":"pattern","color":["#FFFF00"],"pattern":1}}`)
	headerCellStyle, _ := xlsx.NewStyle(`{"fill":{"type":"pattern","color":["#92D050"],"pattern":1}}`)
	tableNameCellStyle, _ := xlsx.NewStyle(`{"fill":{"type":"pattern","color":["#FFC000"],"pattern":1}}`)

	for tableName, value := range extractChangedData {
		ci = 1
		if value == nil {
			// 差分なしのテーブル
			continue
		}
		fmt.Println("===" + tableName + "===")

		// テーブル名出力
		xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), "テーブル名")
		xlsx.SetColWidth(SheetName, excelize.ToAlphaString(ci), excelize.ToAlphaString(ci), 15)
		xlsx.SetCellStyle(SheetName, rowColIndexToAlpha(ri, ci), rowColIndexToAlpha(ri, ci), tableNameCellStyle)
		ci++

		xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), tableName)
		ri++
		ci = 1

		// カラム名出力
		xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), "差分")
		xlsx.SetCellStyle(SheetName, rowColIndexToAlpha(ri, ci), rowColIndexToAlpha(ri, ci), headerCellStyle)

		ci++
		for _, v := range value[0].ColScans {
			xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), v.ColumnName)
			xlsx.SetCellStyle(SheetName, rowColIndexToAlpha(ri, ci), rowColIndexToAlpha(ri, ci), headerCellStyle)
			ci++
		}
		ri++
		ci = 1

		for _, v := range value {
			switch v.DiffStatus {
			case DiffStatusAdd:
				fmt.Printf("INSERTED        :%s\n", v.ColScans)
				ci = 1
				xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), "追加")

				for _, col := range v.ColScans {
					ci++
					xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), col.GetValueString())
				}
			case DiffStatusDel:
				fmt.Printf("DELETED         :%s\n", v.ColScans)
				ci = 1
				xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), "削除")

				for _, col := range v.ColScans {
					ci++
					xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), col.GetValueString())
				}
			case DiffStatusMod:
				ci = 1
				if v.IsBeforeData {
					fmt.Printf("UPDATED[Before] : %s\n", v.ColScans)
					xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), "変更前")
				} else {
					fmt.Printf("UPDATED[After ] : %s\n", v.ColScans)
					xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), "変更後")
				}

				for colIndex, col := range v.ColScans {
					ci++
					for _, value := range v.ModifiedColumnIndex {
						if int(value) == colIndex {
							xlsx.SetCellStyle(SheetName, rowColIndexToAlpha(ri, ci), rowColIndexToAlpha(ri, ci), modCellStyle)
						}
					}
					xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), col.GetValueString())
				}
			case DiffStatusInit:
				fallthrough
			case DiffStatusNotModified:
				fmt.Printf("DiffStatus %d\n", v.DiffStatus)
				continue
			}
			ri++
		}
		ri += 2
	}

	var xmlFilename = "dbdiff_" + time.Now().Format("20060102_150405") + ".xlsx"
	xlsx.SaveAs("./" + xmlFilename)
	fmt.Println("[ResultOutput] See " + xmlFilename)
}

func rowColIndexToAlpha(r int, c int) string {
	s := excelize.ToAlphaString(c) + strconv.Itoa(r)
	return s
}

func collectAllTableData(db *dbdiff.DBManager, tablePks map[string][]string) (*AllTableStore, error) {
	var totalRecordCount uint64 = 0
	var err error
	before := &AllTableStore{AllColumn: map[string][]string{}, AllData: map[string]map[string]*RowObject{}}

	config, err := dbdiff.GetConfiguration()
	if err != nil {
		return nil, err
	}
	schema := config.Db.Schema
	const allDataQueryFormatStr = "SELECT * FROM %s"
	const orderBy = " ORDER BY "
	for tableName, pkColumns := range tablePks {
		// TODO この中goroutine化するとテーブル数多い場合に早くなる？
		query := fmt.Sprintf(allDataQueryFormatStr, schema+tableName)
		if len(pkColumns) > 0 {
			str := orderBy
			for _, v := range pkColumns {
				str += fmt.Sprintf("%s,", v)
			}
			str = strings.TrimRight(str, ",")
			query += str
		}
		rows, err := db.Query(query)
		checkErr(err)

		var tableRows = map[string]*RowObject{}

		columns, err := rows.Columns()
		checkErr(err)

		before.AllColumn[tableName] = columns

		for rows.Next() {
			totalRecordCount++
			//columns, err := rows.Columns()
			//checkErr(err)
			//columnTypes, err := rows.ColumnTypes()
			//checkErr(err)

			var r []*ColumnScan
			for _, columnName := range columns {
				//fmt.Printf("%s: %s, %s, %s\n", columnName, columnTypes[i].Name(), columnTypes[i].DatabaseTypeName(), columnTypes[i].ScanType())
				// 全部文字列で取ってしまう TODO 乱暴？
				// TODO　OracleではNullStringが使えないかも
				var v sql.Scanner
				v = new(sql.NullString)
				var col = &ColumnScan{ColumnName: columnName, Value: v}
				r = append(r, col)
			}
			var r2 []interface{}
			for _, v := range r {
				r2 = append(r2, v)
			}
			err = rows.Scan(r2...)
			checkErr(err)

			rowObject := &RowObject{ColScans: r, DiffStatus: DiffStatusInit, ModifiedColumnIndex: []uint8{}, IsBeforeData: false}
			tableRows[rowObject.GetKey(pkColumns)] = rowObject
		}
		rows.Close()

		//for _, v := range tableRows {
		//	for _, v := range v.ColScans {
		//		fmt.Printf("%s ", v)
		//	}
		//	fmt.Print("\n")
		//}

		before.AllData[tableName] = tableRows
		before.TotalDataCount = totalRecordCount
	}
	return before, err
}

func getPksOfTables(db *dbdiff.DBManager, tableNames []string) (map[string][]string, error) {
	config, err := dbdiff.GetConfiguration()
	if err != nil {
		return nil, err
	}
	schema := config.Db.Schema

	var stmt *sql.Stmt
	if config.Db.DbType == "postgresql" {
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
	} else if config.Db.DbType == "mysql" {
		stmt, err = db.Prepare(`
		SELECT ORDINAL_POSITION, COLUMN_NAME FROM information_schema.columns WHERE table_schema=database() AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI' ORDER BY ORDINAL_POSITION
		`)
	}
	if err != nil {
		return nil, err
	}

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
			// Pkがないので、対象テーブルの全カラム情報を取得して全カラムPKとみなす
			astQuery, err := db.Query("SELECT * FROM " + schema + tableName + " LIMIT 1")
			if err != nil {
				return nil, err
			}
			for astQuery.Next() {
				cols, err := astQuery.Columns()
				if err != nil {
					return nil, err
				}
				columns = append(columns, cols...)
			}
			astQuery.Close()
		}
		tablePks[tableName] = columns
	}
	stmt.Close()
	return tablePks, nil
}

// 全テーブル名を取得
func getAllTables(db *dbdiff.DBManager) ([]string, error) {
	config, err := dbdiff.GetConfiguration()
	if err != nil {
		return nil, err
	}

	var tableNames []string
	var rows *sql.Rows
	if config.Db.DbType == "postgresql" {
		rows, err = db.Query("select relname as TABLE_NAME from pg_stat_user_tables ORDER BY TABLE_NAME")
	} else if config.Db.DbType == "mysql" {
		rows, err = db.Query("SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema=database() ORDER BY TABLE_NAME")
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

// TODO 消したい
func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

type ColumnScan struct {
	ColumnName string
	Value      sql.Scanner
	// TODO メモリ使用量が大きくなるため一旦キャッシュ取るのやめる
	//valueString       string
	//calcedValueString bool
}

func (rs *ColumnScan) String() string {
	var s interface{}
	name := reflect.TypeOf(rs.Value).String()
	v := reflect.ValueOf(rs.Value)
	switch name {
	case "*sql.NullInt64":
		if v.Elem().FieldByName("Valid").Bool() {
			s = strconv.FormatInt(v.Elem().FieldByName("Int64").Int(), 10)
		} else {
			s = "<NULL>"
		}
	case "*sql.NullFloat64":
		if v.Elem().FieldByName("Valid").Bool() {
			s = v.Elem().FieldByName("Float64").Float()
		} else {
			s = "<NULL>"
		}
	case "*sql.NullString":
		fallthrough
	default:
		if v.Elem().FieldByName("Valid").Bool() {
			s = v.Elem().FieldByName("String").String()
		} else {
			s = "<NULL>"
		}
	}
	return fmt.Sprintf("%s:%s", rs.ColumnName, s)
}
func (rs *ColumnScan) GetValueString() string {
	// TODO メモリ使用量が大きくなるため一旦キャッシュ取るのやめる
	//if rs.calcedValueString {
	//	return rs.valueString
	//}
	var s interface{}
	name := reflect.TypeOf(rs.Value).String()
	v := reflect.ValueOf(rs.Value)
	switch name {
	// TODO もっとましなやり方は...
	case "*sql.NullInt64":
		if v.Elem().FieldByName("Valid").Bool() {
			s = strconv.FormatInt(v.Elem().FieldByName("Int64").Int(), 10)
		} else {
			s = "<NULL>"
		}
	case "*sql.NullFloat64":
		if v.Elem().FieldByName("Valid").Bool() {
			s = v.Elem().FieldByName("Float64").Float()
		} else {
			s = "<NULL>"
		}
		// TODO NullStringで入れているので他のcaseは実際には要らない
	case "*sql.NullString":
		fallthrough
	default:
		if v.Elem().FieldByName("Valid").Bool() {
			s = v.Elem().FieldByName("String").String()
		} else {
			s = "<NULL>"
		}
	}
	// TODO メモリ使用量が大きくなるため一旦キャッシュ取るのやめる
	//rs.valueString = fmt.Sprintf("%s", s)
	//rs.calcedValueString = true
	//return rs.valueString
	return fmt.Sprintf("%s", s)
}

func (rs *ColumnScan) Scan(value interface{}) error {
	return rs.Value.Scan(value)
}

type RowObject struct {
	DiffStatus          int8
	ModifiedColumnIndex []uint8
	ColScans            []*ColumnScan
	IsBeforeData        bool
	key                 string
}

func (ro *RowObject) GetKey(pkColumns []string) string {
	if len(ro.key) == 0 {
		var key = ""
		for _, v := range pkColumns {
			for _, v2 := range ro.ColScans {
				if v2.ColumnName == v {
					key += v2.GetValueString()
				}
			}
		}
		ro.key = key
		return key
	} else {
		return ro.key
	}
}
func (ro *RowObject) EqualColumns(that *RowObject) bool {
	if len(ro.ColScans) != len(that.ColScans) {
		// 全カラムを変更扱いにしておく
		for i := 0; i < len(ro.ColScans); i++ {
			ro.ModifiedColumnIndex = append(ro.ModifiedColumnIndex, uint8(i))
		}
		for i := 0; i < len(that.ColScans); i++ {
			that.ModifiedColumnIndex = append(that.ModifiedColumnIndex, uint8(i))
		}
		return false
	}

	result := true
	for index, thatColScan := range that.ColScans {
		// 一致しなかったカラムのindexを保持
		if ro.ColScans[index].GetValueString() != thatColScan.GetValueString() {
			i := uint8(index)
			ro.ModifiedColumnIndex = append(ro.ModifiedColumnIndex, i)
			that.ModifiedColumnIndex = append(that.ModifiedColumnIndex, i)
			result = false
		}
	}
	return result
}

type AllTableStore struct {
	AllData        map[string]map[string]*RowObject
	AllColumn      map[string][]string
	TotalDataCount uint64
}

const (
	DiffStatusInit        int8 = 0 //: 比較前,
	DiffStatusAdd         int8 = 1 //: Add,
	DiffStatusDel         int8 = 2 //: Delete,
	DiffStatusMod         int8 = 3 //: Mod,
	DiffStatusNotModified int8 = 4 //: NotModified
)

// テーブルごとに、追加、変更（変更前後）、削除のデータだけをまとめたものを戻り値で返す
// 呼ぶときは必ず変更前データを引数にし、メッソドレシーバは変更後データとすること
func (ats *AllTableStore) extractChangedData(beforeData *AllTableStore) map[string][]*RowObject {
	var output map[string][]*RowObject = map[string][]*RowObject{}

	for tableName, aTableData := range beforeData.AllData {
		var outputTableData []*RowObject

		afterTableData := ats.AllData[tableName]
		beforeTableData := aTableData

		// key(Pk組み合わせ)でbefore側に存在するキーを保持。後段でafter側にのみあるデータを調べる為に使用
		var scanedKeys = map[string]struct{}{}

		for key, beforeRowObject := range beforeTableData {
			beforeRowObject.IsBeforeData = true

			scanedKeys[key] = struct{}{}
			afterRowObject, ok := afterTableData[key]
			if !ok {
				// afterに要素がないので削除データ
				beforeRowObject.DiffStatus = DiffStatusDel
				outputTableData = append(outputTableData, beforeRowObject)
				continue
			}
			if beforeRowObject.EqualColumns(afterRowObject) {
				// 一致する為変更なし
				beforeRowObject.DiffStatus = DiffStatusNotModified
				afterRowObject.DiffStatus = DiffStatusNotModified
			} else {
				// キーはあるが一致しないので変更
				beforeRowObject.DiffStatus = DiffStatusMod
				outputTableData = append(outputTableData, beforeRowObject)
				afterRowObject.DiffStatus = DiffStatusMod
				outputTableData = append(outputTableData, afterRowObject)
			}
		}

		for key, afterRowObject := range afterTableData {
			_, ok := scanedKeys[key]
			if ok {
				continue
			}
			// 追加されたデータ
			afterRowObject.DiffStatus = DiffStatusAdd
			outputTableData = append(outputTableData, afterRowObject)
		}

		output[tableName] = outputTableData
	}

	return output
}
