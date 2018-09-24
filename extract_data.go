package dbdiff

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func CollectAllTableData(db DbHolder, tablePks map[string][]string) (*AllTableStore, error) {
	var totalRecordCount uint64 = 0
	var err error
	before := &AllTableStore{AllColumn: map[string][]string{}, AllData: map[string]map[string]*RowObject{}}

	config, err := GetConfiguration()
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
		if err != nil {
			return &AllTableStore{}, err
		}

		var tableRows = map[string]*RowObject{}

		columns, err := rows.Columns()
		if err != nil {
			return &AllTableStore{}, err
		}

		before.AllColumn[tableName] = columns

		for rows.Next() {
			totalRecordCount++

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
			if err != nil {
				return &AllTableStore{}, err
			}

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
func (ats *AllTableStore) ExtractChangedData(beforeData *AllTableStore) map[string][]*RowObject {
	var output = map[string][]*RowObject{}

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
