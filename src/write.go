package src

import (
	"encoding/binary"
	"errors"
	"io"
)

var ErrorRowSizeMismatch = errors.New("row size mismatch")

func (stream *Stream) Write(writer io.Writer) error {
	buf := make([]byte, 8)
	// Write the number of rows
	binary.LittleEndian.PutUint64(buf, uint64(len(stream.Values)))
	writer.Write(buf[:8])
	writer.Write(stream.description.Encode())
	for _, r := range stream.Values {
		if len(r) != len(stream.description) {
			return ErrorRowSizeMismatch
		}
		for i, v := range r {
			switch stream.description[i].t {
			case ColumnInt:
				{
					binary.LittleEndian.PutUint64(buf, uint64(v.(int64)))
					if _, err := writer.Write(buf[:8]); err != nil {
						return err
					}
				}
			case ColumnText:
				{
					s := v.(string)
					binary.LittleEndian.PutUint16(buf, uint16(len(s)))
					if _, err := writer.Write(buf[:2]); err != nil {
						return err
					}
					if _, err := writer.Write([]byte(s)); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
