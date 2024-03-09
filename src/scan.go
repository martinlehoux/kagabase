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
	tSize := binary.LittleEndian.Uint64(buf[:8])
	values := make([][]any, 0, tSize)
	// Read the table description
	inDescription, err := DecodeStreamDescription(reader)
	if err != nil {
		return output, err
	}

	outFields := make([]uint8, 0, len(selectedFields))
	output.description = make(StreamDescription, 0, len(outFields))
	if len(selectedFields) == 0 {
		for i := range inDescription {
			outFields = append(outFields, uint8(i))
			output.description = append(output.description, inDescription[i])
		}
	} else {
		for _, fieldName := range selectedFields {
			for i, col := range inDescription {
				if col.name == fieldName {
					outFields = append(outFields, uint8(i))
					output.description = append(output.description, col)
				}
			}
		}
	}

	buf = make([]byte, 65536)
	// Start reading
	for k := 0; k < int(tSize); k++ {
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
		outRow := make([]any, 0, len(output.description))
		for _, i := range outFields {
			outRow = append(outRow, inRow[i])
		}
		values = append(values, outRow)
	}
	output.Values = values
	return output, nil
}
