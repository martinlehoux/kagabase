package src

import (
	"encoding/binary"
	"io"
)

func Scan(reader io.Reader, selectedFields ...string) (Stream, error) {
	var output Stream
	buf := make([]byte, 8)
	// Read the number of rows
	_, err := reader.Read(buf[:8])
	if err != nil {
		return output, err
	}
	numberOfRows := binary.LittleEndian.Uint64(buf[:8])
	outValues := make([][]any, 0, numberOfRows)
	// Read the table description
	inDescription, err := DecodeStreamDescription(reader)
	if err != nil {
		return output, err
	}

	inOutMapping := make([]int16, len(inDescription))
	if len(selectedFields) == 0 {
		output.description = inDescription
		for i := range inDescription {
			inOutMapping[i] = int16(i)
		}
	} else {
		output.description = make(StreamDescription, 0, len(selectedFields))
		for i, col := range inDescription {
			inOutMapping[i] = -1
			for j, fieldName := range selectedFields {
				if col.name == fieldName {
					inOutMapping[i] = int16(j)
					output.description = append(output.description, col)
				}
			}
		}
	}

	buf = make([]byte, 65536)
	// Start reading
	for k := 0; k < int(numberOfRows); k++ {
		inRow := make([]any, 0, len(inDescription))
		for _, col := range inDescription {
			switch col.t {
			case ColumnInt:
				{
					_, err := reader.Read(buf[:8])
					if err != nil {
						return output, err
					}
					inRow = append(inRow, int64(binary.LittleEndian.Uint64(buf[:8])))
				}
			case ColumnText:
				{
					_, err := reader.Read(buf[:2])
					if err != nil {
						return output, err
					}
					textLength := int(binary.LittleEndian.Uint16(buf[:2]))
					_, err = reader.Read(buf[:textLength])
					if err != nil {
						return output, err
					}
					// TODO: Null termination
					inRow = append(inRow, string(buf[:textLength]))
				}
			}
		}
		outRow := make([]any, len(output.description))
		for i, j := range inOutMapping {
			if j != -1 {
				outRow[j] = inRow[i]
			}
		}
		outValues = append(outValues, outRow)
	}
	output.Values = outValues
	return output, nil
}
