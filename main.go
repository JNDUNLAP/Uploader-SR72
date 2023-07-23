package main

import (
	"dunlap/data"
	"io"

	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	- "github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func applycsv(filepath string) (map[string]string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println("Error opening CSV file:", err)
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headerRow, err := reader.Read()
	if err != nil {
		fmt.Println("Error reading CSV header:", err)
		return nil, err
	}

	fmt.Println(headerRow)

	csvData := make([][]string, 0, 10)
	for i := 0; i < 10; i++ {
		row, err := reader.Read()
		if err != nil {
			fmt.Println("Error reading CSV data:", err)
			return nil, err
		}
		csvData = append(csvData, row)
	}

	return data.GetColumnDataTypes(headerRow, csvData), nil
}

func csvTwoTable(filepath string) {
	name := strings.TrimRight(filepath, ".csv")

	table_name := name
	if strings.Contains(name, "/") {
		table_name = strings.Split(name, "/")[1]
	}

	db, err := data.ConnectDatabase()
	if err != nil {
		log.Fatalf("Could not connect to database: %v", err)
		return
	}
	defer db.Close()

	columnDataTypes, err := applycsv(filepath)
	if err != nil {
		log.Fatalf("Could not apply CSV: %v", err)
		return
	}

	columnNamesAndDataTypes := ""
	for columnName, dataType := range columnDataTypes {
		columnNamesAndDataTypes += fmt.Sprintf("%s %s NOT NULL, ", columnName, dataType)
	}

	columnNamesAndDataTypes = strings.TrimRight(columnNamesAndDataTypes, ", ")

	TableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			%s
		);
	`, table_name, columnNamesAndDataTypes)

	fmt.Println(TableSQL)

	_, err = db.Exec(TableSQL)
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}

	fmt.Println("Table created successfully!")
}

func UploadData(file []string) {
	csvTwoTable(file)
	file, err := os.Open(file)
	r := csv.NewReader(file)


	db, err := data.ConnectDatabase()
	if err != nil {
		log.Fatalf("Could not connect to database: %v", err)
		return
	}


	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		tx := db.MustBegin()
		tx.MustExec("INSERT INTO yourtable (column1, column2) VALUES ($1, $2)", record[0], record[1])
		tx.Commit()
	}
}

func main() {
	UploadData("test/VA.csv")
}
