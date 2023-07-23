package data

import (
	"strconv"
	"time"
)

func InferDataType(value string) string {
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return "INT"
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return "DECIMAL"
	}

	dateFormats := []string{
		"2006-01-02",
		"01/02/2006",
		"1/2/2006",
		"01-02-2006",
		"02-01-2006",
		"Jan 2, 2006",
		"Jan-02-06",
		"02 Jan 2006",
		"January 2, 2006",
		"02-Jan-2006",
		"02-January-2006",
	}

	for _, format := range dateFormats {
		if _, err := time.Parse(format, value); err == nil {
			return "DATE"
		}
	}

	return "TEXT"
}
