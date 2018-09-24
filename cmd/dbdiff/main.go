package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/jparound30/dbdiff"
	"log"
	_ "net/http/pprof"
	"os"
	"strconv"
	"time"
)

const (
	DefaultConfigurationYaml    = "configuration.yaml"
	DefaultOutputResultFilename = "dbdiff_yyyymmdd_hhmmss.xlsx"
)

func main() {
	// Parse arguments
	flag.CommandLine.Init(os.Args[0], flag.ExitOnError)

	var configFilePath string
	flag.StringVar(&configFilePath, "conf", DefaultConfigurationYaml, "Specify path of configuration file.")
	var outputFileName string
	flag.StringVar(&outputFileName, "o", DefaultOutputResultFilename, "Filename of result file(.xlsx).")

	flag.Parse()

	configuration, err := dbdiff.LoadConfiguration(configFilePath)
	if err != nil {
		log.Fatal("Failed to load configuration file.")
	}
	db, err := dbdiff.GetDBInstance(&configuration.Db)
	if err != nil {
		log.Fatal("DB instance initialization failed.")
	}
	defer db.Finalize()

	fmt.Println("[INITIALIZING] Collecting Table Information ...")
	tableNames, err := dbdiff.GetAllTables(db, configuration)
	checkErr(err)

	tablePks, err := dbdiff.GetPksOfTables(db, configuration, tableNames)
	checkErr(err)

	//for key, value := range tablePks {
	//	fmt.Printf("TABLE:%s, PK_COLUMN:%s\n", key, value)
	//}

	fmt.Print("[BEFORE] Collecting snapshot data...")
	before, err := dbdiff.CollectAllTableData(db, tablePks)
	checkErr(err)
	fmt.Printf(", Total record count: %d ...", before.TotalDataCount)
	fmt.Println(" COMPLETE!")

	fmt.Printf("OK, Let's do some operations, THEN HIT ANY KEY!")
	stdin := bufio.NewScanner(os.Stdin)
	stdin.Scan()

	fmt.Print("\n[AFTER ] Collecting snapshot data...")
	after, err := dbdiff.CollectAllTableData(db, tablePks)
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

	extractChangedData := after.ExtractChangedData(before)
	outputResultToExcelFile(extractChangedData, outputFileName)
}

func outputResultToExcelFile(extractChangedData map[string][]*dbdiff.RowObject, outputFileName string) {
	// TODO Excel出力　要refactoring
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
			case dbdiff.DiffStatusAdd:
				fmt.Printf("INSERTED        :%s\n", v.ColScans)
				ci = 1
				xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), "追加")

				for _, col := range v.ColScans {
					ci++
					xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), col.GetValueString())
				}
			case dbdiff.DiffStatusDel:
				fmt.Printf("DELETED         :%s\n", v.ColScans)
				ci = 1
				xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), "削除")

				for _, col := range v.ColScans {
					ci++
					xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), col.GetValueString())
				}
			case dbdiff.DiffStatusMod:
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
			case dbdiff.DiffStatusInit:
				fallthrough
			case dbdiff.DiffStatusNotModified:
				fmt.Printf("DiffStatus %d\n", v.DiffStatus)
				continue
			}
			ri++
		}
		ri += 2
	}
	var xlsxFilename string
	if outputFileName == DefaultOutputResultFilename {
		// default filename
		xlsxFilename = "dbdiff_" + time.Now().Format("20060102_150405") + ".xlsx"
	} else {
		xlsxFilename = outputFileName
	}
	xlsx.SaveAs(xlsxFilename)
	fmt.Println("[ResultOutput] See " + xlsxFilename)
}

func rowColIndexToAlpha(r int, c int) string {
	s := excelize.ToAlphaString(c) + strconv.Itoa(r)
	return s
}

// TODO 消したい
func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
