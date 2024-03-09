package src_test

import (
	"bytes"
	"testing"

	"github.com/martinlehoux/kagabase/src"
	"github.com/stretchr/testify/assert"
)

func TestTableDescription(t *testing.T) {
	description := src.TableDescription{src.ColumnInt, src.ColumnText}
	encoded := description.Encode()
	decoded, err := src.DecodeDescription(bytes.NewReader(encoded))
	assert.NoError(t, err)
	assert.Equal(t, description, decoded)

}

func TestWriteReadOneInteger(t *testing.T) {
	type row struct {
		Col1 int
	}
	file := bytes.NewBuffer([]byte(""))
	description := []src.ColumnType{src.ColumnInt}
	data := []row{{1}, {2}, {3}}

	err := src.Write(description, file, data)
	assert.NoError(t, err)

	values, err := src.Read[row](file)
	assert.NoError(t, err)
	assert.Equal(t, data, values)
}

func TestWriteReadTwoIntegers(t *testing.T) {
	type row struct {
		Col1 int
		Col2 int
	}
	file := bytes.NewBuffer([]byte(""))
	description := []src.ColumnType{src.ColumnInt, src.ColumnInt}
	data := []row{{1, 2}, {3, 4}, {5, 6}}

	err := src.Write(description, file, data)
	assert.NoError(t, err)

	values, err := src.Read[row](file)
	assert.NoError(t, err)
	assert.Equal(t, data, values)
}

func TestWriteReadTextAndInteger(t *testing.T) {
	type row struct {
		Col1 string
		Col2 int
	}
	file := bytes.NewBuffer([]byte(""))
	description := []src.ColumnType{src.ColumnText, src.ColumnInt}
	data := []row{{"a", 1}, {"b", 2}, {"fbd0b811-78e4-4c2f-b96d-0223818dc153", 3}}

	err := src.Write(description, file, data)
	assert.NoError(t, err)

	values, err := src.Read[row](file)
	assert.NoError(t, err)
	assert.Equal(t, data, values)
}

func BenchmarkReadOneInteger(b *testing.B) {
	file := bytes.NewBuffer([]byte(""))
	type row struct {
		Col1 int
	}
	description := []src.ColumnType{src.ColumnInt}
	data := []row{}
	for i := 0; i < b.N; i++ {
		data = append(data, row{i})
	}
	src.Write(description, file, data)
	b.ResetTimer()

	src.Read[row](file)
}
