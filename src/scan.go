package src

import (
	"encoding/binary"
	"io"
)

func Scan(reader io.Reader, selectedFields ...string) (Stream, error) {
	var output Stream
	intBuf := [8]byte{}
	// Read the number of rows
	_, err := reader.Read(intBuf[:])
	if err != nil {
		return output, err
	}
	numberOfRows := binary.LittleEndian.Uint64(intBuf[:])
	outValues := make([][]any, numberOfRows)
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

	textBuf := [65536]byte{}
	// Start reading
	for k := 0; k < int(numberOfRows); k++ {
		outRow := make([]any, len(output.description))
		for inColNumber, col := range inDescription {
			switch col.t {
			case ColumnInt:
				{
					_, err := reader.Read(intBuf[:8])
					if err != nil {
						return output, err
					}
					if outColNumber := inOutMapping[inColNumber]; outColNumber != -1 {
						outRow[outColNumber] = int64(binary.LittleEndian.Uint64(intBuf[:8]))
					}
				}
			case ColumnText:
				{
					_, err := reader.Read(intBuf[:2])
					if err != nil {
						return output, err
					}
					textLength := int(binary.LittleEndian.Uint16(intBuf[:2]))
					_, err = reader.Read(textBuf[:textLength])
					if err != nil {
						return output, err
					}
					if outColNumber := inOutMapping[inColNumber]; outColNumber != -1 {
						// TODO: Null termination
						outRow[outColNumber] = string(textBuf[:textLength])
					}
				}
			}
		}
		outValues[k] = outRow
	}
	output.Values = outValues
	return output, nil
}
