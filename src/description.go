package src

import "io"

type ColumnType uint8

const (
	ColumnInt  ColumnType = 1 // 8 bytes
	ColumnText ColumnType = 2 // 2 bytes for the length + n bytes for the text (max 65536)
)

type TableDescription []ColumnType

func (d TableDescription) Encode() []byte {
	buf := make([]byte, len(d)+1)
	buf[0] = byte(len(d))
	for i, c := range d {
		buf[i+1] = byte(c)
	}
	return buf
}

func DecodeDescription(reader io.Reader) (TableDescription, error) {
	d := TableDescription{}
	// Read the number of columns
	ncb := make([]byte, 1)
	_, err := reader.Read(ncb)
	if err != nil {
		return d, err
	}
	nc := uint8(ncb[0])
	// Read the column types
	buf := make([]byte, nc)
	_, err = reader.Read(buf)
	if err != nil {
		return d, err
	}
	for i := uint8(0); i < nc; i++ {
		d = append(d, ColumnType(buf[i]))
	}
	return d, nil
}

func (d TableDescription) FromStruct() error {
	return nil
}
