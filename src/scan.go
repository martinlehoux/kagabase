package src

import (
	"encoding/binary"
	"io"
)

type StreamHeader struct {
	numberOfRows uint64
	description  StreamDescription
}

func decodeHeader(reader io.Reader) (StreamHeader, error) {
	var header StreamHeader
	intBuf := [8]byte{}
	_, err := reader.Read(intBuf[:])
	if err != nil {
		return header, err
	}
	header.numberOfRows = binary.LittleEndian.Uint64(intBuf[:])
	inDescription, err := DecodeStreamDescription(reader)
	if err != nil {
		return header, err
	}
	header.description = inDescription

	return header, nil
}

func selectFields(inDescription StreamDescription, selectedFields []string) (StreamDescription, []int16) {
	inOutMapping := make([]int16, len(inDescription))
	if len(selectedFields) == 0 {
		outDescription := make(StreamDescription, len(inDescription))
		copy(outDescription, inDescription)
		for i := range inDescription {
			inOutMapping[i] = int16(i)
		}
		return outDescription, inOutMapping
	}
	outDescription := make(StreamDescription, 0, len(selectedFields))
	for i, col := range inDescription {
		inOutMapping[i] = -1
		for j, fieldName := range selectedFields {
			if col.name == fieldName {
				inOutMapping[i] = int16(j)
				outDescription = append(outDescription, col)
			}
		}
	}
	return outDescription, inOutMapping
}

func SeqScan(reader io.Reader, selectedFields ...string) (Stream, error) {
	var output Stream
	header, err := decodeHeader(reader)
	if err != nil {
		return output, err
	}

	outDescription, inOutMapping := selectFields(header.description, selectedFields)
	output.description = outDescription

	outValues := make([][]any, header.numberOfRows)
	textBuf := [columnTextMaxLength]byte{}
	intBuf := [8]byte{}
	// Start reading
	for k := 0; k < int(header.numberOfRows); k++ {
		outRow := make([]any, len(output.description))
		for inColNumber, col := range header.description {
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

func ParallelScan(reader io.Reader, selectedFields ...string) (Stream, error) {
	return Stream{}, nil
}
