package data

func GetColumnDataTypes(headerRow []string, csvData [][]string) map[string]string {
	columnDataTypes := make(map[string]string)
	for _, header := range headerRow {
		columnDataTypes[header] = ""
	}

	for _, row := range csvData {
		for i, cellValue := range row {
			header := headerRow[i]

			if cellValue == "" {
				continue
			}

			currentType := columnDataTypes[header]

			cellType := InferDataType(cellValue) // note the capitalization

			if currentType == "" || (currentType != "string" && currentType != cellType) {
				columnDataTypes[header] = cellType
			}
		}
	}

	return columnDataTypes
}
