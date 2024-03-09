package src_test

import (
	"bytes"
	"testing"

	"github.com/martinlehoux/kagabase/src"
	"github.com/stretchr/testify/assert"
)

func TestWriteReadOneInteger(t *testing.T) {
	file := bytes.NewBuffer([]byte(""))
	table := src.Table{Writer: file, Reader: file}
	data := [][]int{{1}, {2}, {3}}

	err := table.Write(data, 1)
	assert.NoError(t, err)

	values, err := table.Read()
	assert.NoError(t, err)
	assert.Equal(t, data, values)
}

func TestWriteReadTwoIntegers(t *testing.T) {
	file := bytes.NewBuffer([]byte(""))
	table := src.Table{Writer: file, Reader: file}
	data := [][]int{{1, 2}, {3, 4}, {5, 6}}

	err := table.Write(data, 2)
	assert.NoError(t, err)

	values, err := table.Read()
	assert.NoError(t, err)
	assert.Equal(t, data, values)
}

func BenchmarkReadOneInteger(b *testing.B) {
	file := bytes.NewBuffer([]byte(""))
	table := src.Table{Writer: file, Reader: file}
	data := [][]int{}
	for i := 0; i < b.N; i++ {
		data = append(data, []int{i})
	}
	table.Write(data, 1)
	b.ResetTimer()

	table.Read()
}
