package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/jparound30/dbdiff"
	"github.com/xuri/excelize/v2"
	"log"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"time"
)

const (
	DefaultConfigurationYaml    = "configuration.yaml"          // default configuration filename
	DefaultOutputResultFilename = "dbdiff_yyyymmdd_hhmmss.xlsx" // default output filename
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
	fmt.Printf("Table count: %d\n", len(tableNames))

	tablePks, err := dbdiff.GetPksOfTables(db, configuration, tableNames)
	checkErr(err)
	//for key, value := range tablePks {
	//	fmt.Printf("TABLE:%s, PK_COLUMN:%s\n", key, value)
	//}

	fmt.Print("[BEFORE] Collecting snapshot data...")
	before := dbdiff.AllTableStore{}
	err = before.CollectAllTableData(db, configuration, tablePks)
	checkErr(err)
	fmt.Printf(", Total record count: %d ...", before.TotalDataCount)
	fmt.Println(" COMPLETE!")

	stdin := bufio.NewScanner(os.Stdin)

	printMemStat()
	fmt.Printf("OK, Let's do some operations, THEN HIT ANY KEY! OR type 'q' or 'exit' to quit this tool.  ")
	for stdin.Scan() {
		input := stdin.Text()
		if input == "q" || input == "exit" {
			break
		}

		fmt.Print("\n[AFTER ] Collecting snapshot data...")
		after := dbdiff.AllTableStore{}
		err = after.CollectAllTableData(db, configuration, tablePks)
		checkErr(err)
		fmt.Printf(", Total record count: %d ...", after.TotalDataCount)
		fmt.Println("COMPLETE!")

		extractChangedData := after.ExtractChangedData(&before)
		outputResultToExcelFile(extractChangedData, outputFileName)

		// swap
		before = after

		printMemStat()
		fmt.Printf("OK, Let's do some operations, THEN HIT ANY KEY! OR type 'q' or 'exit' to quit this tool.  ")
	}

	// TODO プロファイル用 そのうち削除
	//var wg sync.WaitGroup
	//
	//go func() {
	//	log.Println(http.ListenAndServe(":6060", nil))
	//}()
	//
	//wg.Add(1)
	//wg.Wait()
}

const (
	DiffResultOffsetForColumn = 2 // "B"
	DiffResultOffsetForRow    = 2 // "2"
	DiffResultMargin          = 2 // Margin between tables

	SheetName = "Sheet1"
)

func outputResultToExcelFile(extractChangedData map[string][]*dbdiff.RowObject, outputFileName string) {
	// TODO Excel出力　要refactoring
	var err error
	xlsx := excelize.NewFile()
	xlsx.NewSheet(SheetName)

	var ri = DiffResultOffsetForRow
	var ci = DiffResultOffsetForColumn

	var modBorderColor = "#FF0000"
	var commonBorderStyle = 1
	modCellStyle, err := xlsx.NewStyle(
		&excelize.Style{
			Fill: excelize.Fill{
				Type:    "pattern",
				Color:   []string{"#FFFF00"},
				Pattern: 1,
			},
			Border: []excelize.Border{
				excelize.Border{Type: "left", Color: modBorderColor, Style: commonBorderStyle},
				excelize.Border{Type: "top", Color: modBorderColor, Style: commonBorderStyle},
				excelize.Border{Type: "right", Color: modBorderColor, Style: commonBorderStyle},
				excelize.Border{Type: "bottom", Color: modBorderColor, Style: commonBorderStyle},
			},
		})

	checkErr(err)

	var unmodBorderColor = "#000000"
	unmodCellStyle, err := xlsx.NewStyle(
		&excelize.Style{
			Border: []excelize.Border{
				excelize.Border{Type: "left", Color: unmodBorderColor, Style: commonBorderStyle},
				excelize.Border{Type: "top", Color: unmodBorderColor, Style: commonBorderStyle},
				excelize.Border{Type: "right", Color: unmodBorderColor, Style: commonBorderStyle},
				excelize.Border{Type: "bottom", Color: unmodBorderColor, Style: commonBorderStyle},
			},
		})

	var headerBorderColor = "#000000"
	headerCellStyle, err := xlsx.NewStyle(
		&excelize.Style{
			Fill: excelize.Fill{
				Type:    "pattern",
				Color:   []string{"#92D050"},
				Pattern: 1,
			},
			Border: []excelize.Border{
				excelize.Border{Type: "left", Color: headerBorderColor, Style: commonBorderStyle},
				excelize.Border{Type: "top", Color: headerBorderColor, Style: commonBorderStyle},
				excelize.Border{Type: "right", Color: headerBorderColor, Style: commonBorderStyle},
				excelize.Border{Type: "bottom", Color: headerBorderColor, Style: commonBorderStyle},
			},
		})
	checkErr(err)
	tableNameCellStyle, err := xlsx.NewStyle(
		&excelize.Style{
			Fill: excelize.Fill{
				Type:    "pattern",
				Color:   []string{"#FFC000"},
				Pattern: 1,
			},
		})
	checkErr(err)

	for tableName, value := range extractChangedData {

		ci = DiffResultOffsetForColumn
		if value == nil {
			// table no differences
			continue
		}
		fmt.Println("===" + tableName + "===")

		///////
		// Table name
		///////
		colName, _ := excelize.ColumnNumberToName(ci)
		// テーブル名出力
		err = xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), "TableName")
		checkErr(err)
		err = xlsx.SetColWidth(SheetName, colName, colName, 15)
		checkErr(err)
		err = xlsx.SetCellStyle(SheetName, rowColIndexToAlpha(ri, ci), rowColIndexToAlpha(ri, ci), tableNameCellStyle)
		checkErr(err)

		ci++
		err = xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), tableName)
		checkErr(err)

		///////
		// Header ( Column names )
		///////
		ri++
		ci = DiffResultOffsetForColumn

		xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), "(diff)")
		xlsx.SetCellStyle(SheetName, rowColIndexToAlpha(ri, ci), rowColIndexToAlpha(ri, ci), headerCellStyle)

		ci++
		for _, colName := range value[0].ColumnNames {
			xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), colName)
			xlsx.SetCellStyle(SheetName, rowColIndexToAlpha(ri, ci), rowColIndexToAlpha(ri, ci), headerCellStyle)
			ci++
		}

		ri++
		ci = DiffResultOffsetForColumn

		for _, v := range value {
			switch v.DiffStatus {
			case dbdiff.DiffStatusAdd:
				fmt.Printf("INSERTED        : %s\n", v)
				ci = DiffResultOffsetForColumn
				xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), "INSERTED")
				xlsx.SetCellStyle(SheetName, rowColIndexToAlpha(ri, ci), rowColIndexToAlpha(ri, ci), unmodCellStyle)
				for _, col := range v.ColScans {
					ci++
					xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), col.GetValueString())
					xlsx.SetCellStyle(SheetName, rowColIndexToAlpha(ri, ci), rowColIndexToAlpha(ri, ci), unmodCellStyle)
				}
			case dbdiff.DiffStatusDel:
				fmt.Printf("DELETED         : %s\n", v)
				ci = DiffResultOffsetForColumn
				xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), "DELETED")
				xlsx.SetCellStyle(SheetName, rowColIndexToAlpha(ri, ci), rowColIndexToAlpha(ri, ci), unmodCellStyle)

				for _, col := range v.ColScans {
					ci++
					xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), col.GetValueString())
					xlsx.SetCellStyle(SheetName, rowColIndexToAlpha(ri, ci), rowColIndexToAlpha(ri, ci), unmodCellStyle)
				}
			case dbdiff.DiffStatusMod:
				ci = DiffResultOffsetForColumn
				if v.IsBeforeData {
					fmt.Printf("UPDATED[Before] : %s\n", v)
					xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), "UPD BEFORE")
				} else {
					fmt.Printf("UPDATED[After ] : %s\n", v)
					xlsx.SetCellStr(SheetName, rowColIndexToAlpha(ri, ci), "UPD  AFTER")
				}
				xlsx.SetCellStyle(SheetName, rowColIndexToAlpha(ri, ci), rowColIndexToAlpha(ri, ci), unmodCellStyle)

				for colIndex, col := range v.ColScans {
					ci++
					xlsx.SetCellStyle(SheetName, rowColIndexToAlpha(ri, ci), rowColIndexToAlpha(ri, ci), unmodCellStyle)
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
		ri += DiffResultMargin
	}

	xlsxFilename := generateOutFilename(outputFileName)
	xlsx.SaveAs(xlsxFilename)
	fmt.Println("[ResultOutput] See " + xlsxFilename)

	// EXCELファイルを表示する
	if runtime.GOOS == "darwin" {
		if err := exec.Command("/usr/bin/open", xlsxFilename).Start(); err != nil {
			log.Fatalf("err = %v", err)
		}
	} else if runtime.GOOS == "windows" {
		if err := exec.Command("cmd", "/C", xlsxFilename).Start(); err != nil {
			log.Fatalf("err = %v", err)
		}
	}
}

// Generate Output filename.
func generateOutFilename(specifiedFilename string) string {
	var xlsxFilename string
	if specifiedFilename == DefaultOutputResultFilename {
		// default filename
		xlsxFilename = "dbdiff_" + time.Now().Format("20060102_150405") + ".xlsx"
	} else {
		xlsxFilename = specifiedFilename
	}
	return xlsxFilename
}

// Convert (int, int) to "A1" format string
//
// r and c is [1..]
func rowColIndexToAlpha(r int, c int) string {
	if colName, err := excelize.ColumnNumberToName(c); err != nil {
		log.Fatalf("Invalid row,column # : [r:%d, c:%d]", r, c)
		return "" // unreachable
	} else {
		return colName + strconv.Itoa(r)
	}
}

// TODO 消したい
func checkErr(err error) {
	if err != nil {
		log.Fatalf("ERROR : %v", err)
	}
}

func printMemStat() {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	heapAllocMegas := float64(mem.HeapAlloc) / float64(1024*1024)
	fmt.Printf("Heap memory usage: %.3f MB\n", heapAllocMegas)
}
