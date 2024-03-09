package src_test

import (
	"bytes"
	"testing"

	"github.com/martinlehoux/kagabase/src"
	"github.com/stretchr/testify/assert"
)

func TestWriteScanOneInteger(t *testing.T) {
	type row struct {
		Col1 int
	}
	file := bytes.NewBuffer([]byte(""))
	description := src.TableDescription{}.Add("col_1", src.ColumnInt)
	data := []row{{1}, {2}, {3}}

	err := src.Write(description, file, data)
	assert.NoError(t, err)

	values, err := src.Scan[row](file)
	assert.NoError(t, err)
	assert.Equal(t, data, values)
}

func TestWriteScanTwoIntegers(t *testing.T) {
	type row struct {
		Col1 int
		Col2 int
	}
	file := bytes.NewBuffer([]byte(""))
	description := src.TableDescription{}.Add("col_1", src.ColumnInt).Add("col_2", src.ColumnInt)
	data := []row{{1, 2}, {3, 4}, {5, 6}}

	err := src.Write(description, file, data)
	assert.NoError(t, err)

	values, err := src.Scan[row](file)
	assert.NoError(t, err)
	assert.Equal(t, data, values)
}

func TestWriteScanTextAndInteger(t *testing.T) {
	type row struct {
		Col1 string
		Col2 int
	}
	file := bytes.NewBuffer([]byte(""))
	description := src.TableDescription{}.Add("col_1", src.ColumnText).Add("col_2", src.ColumnInt)
	data := []row{{"a", 1}, {"b", 2}, {"fbd0b811-78e4-4c2f-b96d-0223818dc153", 3}}

	err := src.Write(description, file, data)
	assert.NoError(t, err)

	values, err := src.Scan[row](file)
	assert.NoError(t, err)
	assert.Equal(t, data, values)
}

func BenchmarkScanOneInteger(b *testing.B) {
	file := bytes.NewBuffer([]byte(""))
	type row struct {
		Col1 int
	}
	description := src.TableDescription{}.Add("col_1", src.ColumnInt)
	data := make([]row, b.N)
	for i := 0; i < b.N; i++ {
		data = append(data, row{i})
	}
	src.Write(description, file, data)
	b.ResetTimer()

	src.Scan[row](file)
}
