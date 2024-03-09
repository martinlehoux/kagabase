package src

import (
	"encoding/binary"
	"errors"
	"io"
	"reflect"
)

var ErrorRowSizeMismatch = errors.New("row size mismatch")

func Write[row any](description TableDescription, writer io.Writer, values []row) error {
	buf := make([]byte, 8)
	// Write the number of rows
	binary.LittleEndian.PutUint64(buf, uint64(len(values)))
	writer.Write(buf[:8])
	writer.Write(description.Encode())
	for _, r := range values {
		v := reflect.ValueOf(r)
		if v.NumField() != len(description) {
			return ErrorRowSizeMismatch
		}
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			switch field.Kind() {
			case reflect.Int:
				{
					binary.LittleEndian.PutUint64(buf, uint64(field.Int()))
					if _, err := writer.Write(buf[:8]); err != nil {
						return err
					}
				}
			case reflect.String:
				{
					s := field.String()
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

func Scan[row any](reader io.Reader) ([]row, error) {
	buf := make([]byte, 8)
	// Read the number of rows
	_, err := reader.Read(buf[:8])
	if err != nil {
		return nil, err
	}
	tSize := binary.LittleEndian.Uint64(buf[:8])
	values := make([]row, 0, tSize)
	// Read the table description
	d, err := DecodeDescription(reader)
	if err != nil {
		return nil, err
	}
	buf = make([]byte, 65536)
	var emptyR row
	rType := reflect.TypeOf(emptyR)
	fields := make([]reflect.StructField, len(d))
	for i := 0; i < len(d); i++ {
		fields[i] = rType.Field(i)
	}
	for k := 0; k < int(tSize); k++ {
		var row row
		for i := 0; i < len(d); i++ {
			field := fields[i]
			rowV := reflect.ValueOf(&row).Elem().Field(i)
			switch field.Type.Kind() {
			case reflect.Int:
				{
					_, err := reader.Read(buf[:8])
					if err != nil {
						return nil, err
					}
					rowV.SetInt(int64(binary.LittleEndian.Uint64(buf[:8])))
				}
			case reflect.String:
				{
					_, err := reader.Read(buf[:2])
					if err != nil {
						return nil, err
					}
					textLength := int(binary.LittleEndian.Uint16(buf[:2]))
					_, err = reader.Read(buf[:textLength])
					if err != nil {
						return nil, err
					}
					rowV.SetString(string(buf[:textLength]))
				}
			}
		}
		values = append(values, row)
	}
	return values, nil
}
