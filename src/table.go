package src

import (
	"encoding/binary"
	"errors"
	"io"
)

type ColumnType uint

const (
	ColumnInt  ColumnType = 1
	ColumnText ColumnType = 2
)

type Table struct {
	Description []ColumnType
	Writer      io.Writer
	Reader      io.Reader
}

var ErrorRowSizeMismatch = errors.New("row size mismatch")

func (t *Table) Write(values [][]int, rSize uint) error {
	buf := make([]byte, 8)
	// Write the table description
	t.Writer.Write([]byte{byte(rSize)})
	// Write the number of rows
	binary.LittleEndian.PutUint64(buf, uint64(len(values)))
	t.Writer.Write(buf)
	for _, r := range values {
		if uint(len(r)) != rSize {
			return ErrorRowSizeMismatch
		}
		for _, v := range r {
			binary.LittleEndian.PutUint64(buf, uint64(v))
			if _, err := t.Writer.Write(buf); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Table) Read() ([][]int, error) {
	buf := make([]byte, 8)
	// Read the table description
	_, err := t.Reader.Read(buf[:1])
	if err != nil {
		return nil, err
	}
	rSize := uint(buf[0])
	buf = make([]byte, 8*rSize)
	// Read the number of rows
	_, err = t.Reader.Read(buf[:8])
	if err != nil {
		return nil, err
	}
	tSize := binary.LittleEndian.Uint64(buf[:8])
	values := make([][]int, 0, tSize)
	for {
		_, err := t.Reader.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		row := []int{}
		for i := 0; i < len(buf); i += 8 {
			row = append(row, int(binary.LittleEndian.Uint64(buf[i:i+8])))
		}
		values = append(values, row)
	}
	return values, nil
}
