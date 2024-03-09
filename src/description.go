package src

import (
	"bytes"
	"io"
)

type ColumnType uint8

const (
	ColumnInt  ColumnType = 1 // 8 bytes
	ColumnText ColumnType = 2 // 2 bytes for the length + n bytes for the text (max 65536)
)

type TableColumn struct {
	t    ColumnType // 1 byte
	name string     // 256 bytes for 32 characters
}

type TableDescription []TableColumn

func (d TableDescription) Add(name string, t ColumnType) TableDescription {
	return append(d, TableColumn{name: name, t: t})
}

func (d TableDescription) Encode() []byte {
	buf := make([]byte, len(d)*(1+256)+1)
	buf[0] = byte(len(d))
	for i, c := range d {
		offset := 1 + i*(1+256)
		buf[offset] = byte(c.t)
		copy(buf[offset+1:offset+256+1], c.name)
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
	buf := make([]byte, (256+1)*uint(nc))
	_, err = reader.Read(buf)
	if err != nil {
		return d, err
	}
	for i := uint8(0); i < nc; i++ {
		offset := uint(i) * (256 + 1)
		var c TableColumn
		c.t = ColumnType(buf[offset])
		c.name = string(bytes.TrimRight(buf[offset+1:offset+256+1], "\x00"))
		d = append(d, c)
	}
	return d, nil
}

func (d TableDescription) FromStruct() error {
	return nil
}
