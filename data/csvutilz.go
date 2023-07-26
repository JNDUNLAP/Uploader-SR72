package data

import (
	"encoding/csv"
	"log"
	"strings"
)

func TableName(filepath string) string {
	name := strings.TrimRight(filepath, ".csv")

	table_name := name
	if strings.Contains(name, "/") {
		table_name = strings.Split(name, "/")[1]
	}
	return table_name
}

func SkipCSVHeader(r *csv.Reader) error {
	_, err := r.Read()
	if err != nil {
		log.Printf("Error skipping header row: %v", err)
		return err
	}
	return nil
}
