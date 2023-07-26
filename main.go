package main

import (
	"dunlap/data"
	"io"

	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/lib/pq"
)

func applycsv(filepath string) (map[string]string, []string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println("Error opening CSV file:", err)
		return nil, nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headerRow, err := reader.Read()
	if err != nil {
		fmt.Println("Error reading CSV header:", err)
		return nil, nil, err
	}
	depth := 3
	csvData := make([][]string, 0, depth)
	for i := 0; i < depth; i++ {
		row, err := reader.Read()
		if err != nil {
			fmt.Println("Error reading CSV data:", err)
			return nil, nil, err
		}
		csvData = append(csvData, row)
	}

	return data.GetColumnDataTypes(headerRow, csvData), headerRow, nil
}

func csvTwoTable(filepath string) (map[string]string, []string, string, error) {
	table_name := data.TableName(filepath)

	db, err := data.ConnectDatabase()
	if err != nil {
		log.Printf("Could not connect to database: %v", err)
		return nil, nil, "", err
	}
	defer db.Close()

	columnDataTypes, columnOrder, err := applycsv(filepath)
	if err != nil {
		log.Printf("Could not apply CSV: %v", err)
		return nil, nil, "", err
	}
	fmt.Printf("Column data types: %v\n", columnDataTypes)
	fmt.Printf("Column order: %v\n", columnOrder)

	// Ensure columnOrder is not empty to avoid panic
	if len(columnOrder) == 0 {
		return nil, nil, "", fmt.Errorf("no columns in the CSV file")
	}

	// Make the first column a PRIMARY KEY
	columnNamesAndDataTypes := fmt.Sprintf("%s %s PRIMARY KEY, ", columnOrder[0], columnDataTypes[columnOrder[0]])

	for _, columnName := range columnOrder[1:] { // Start from 1 to skip first column
		dataType := columnDataTypes[columnName]
		columnNamesAndDataTypes += fmt.Sprintf("%s %s NOT NULL, ", columnName, dataType)
	}

	columnNamesAndDataTypes = strings.TrimRight(columnNamesAndDataTypes, ", ")

	TableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			%s
		);
	`, table_name, columnNamesAndDataTypes)

	_, err = db.Exec(TableSQL)
	if err != nil {
		log.Printf("Error creating table: %v", err)
		return nil, nil, "", err
	}

	fmt.Println("Table created successfully!")
	return columnDataTypes, columnOrder, table_name, nil
}

func UploadData(filepath string) {
	columnDataTypes, columnOrder, tableName, err := csvTwoTable(filepath)
	if err != nil {
		log.Fatalf("Error creating table from CSV: %v", err)
		return
	}
	fmt.Println(columnDataTypes)

	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
		return
	}
	defer file.Close()

	r := csv.NewReader(file)

	db, err := data.ConnectDatabase()
	if err != nil {
		log.Fatalf("Could not connect to database: %v", err)
		return
	}
	defer db.Close()

	err = data.SkipCSVHeader(r)
	if err != nil {
		log.Fatalf("Could not skip header row: %v", err)
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

		valuePlaceholders := make([]string, len(record))
		for i := range record {
			valuePlaceholders[i] = fmt.Sprintf("$%d", i+1)
		}
		setClause := ""
		for _, columnName := range columnOrder[1:] {
			setClause += fmt.Sprintf("%s = excluded.%s, ", columnName, columnName)
		}
		setClause = strings.TrimRight(setClause, ", ")

		query := fmt.Sprintf(
			`INSERT INTO %s (%s) VALUES (%s) 
			ON CONFLICT (%s) DO UPDATE SET %s`,
			tableName,
			strings.Join(columnOrder, ", "),
			strings.Join(valuePlaceholders, ", "),
			columnOrder[0],
			setClause,
		)
		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}

		recordInterface := make([]interface{}, len(record))
		for i, v := range record {
			recordInterface[i] = v
		}

		_, err = tx.Exec(query, recordInterface...)
		if err != nil {
			_ = tx.Rollback()
			log.Fatal(err)
		} else {
			err = tx.Commit()
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func main() {
	UploadData("header.csv")

}
