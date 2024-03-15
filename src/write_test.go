package src_test

import (
	"bytes"
	"testing"

	"github.com/martinlehoux/kagabase/src"
	"github.com/stretchr/testify/assert"
)

func TestWriteScanOneInteger(t *testing.T) {
	file := bytes.NewBuffer([]byte(""))
	description := src.StreamDescription{}.Add("col_1", src.ColumnInt)
	inStream := src.NewStream(description, [][]any{{int64(1)}, {int64(2)}, {int64(3)}})

	err := src.Write(file, inStream)
	assert.NoError(t, err)

	outStream, err := src.Scan(file)
	assert.NoError(t, err)
	assert.Equal(t, inStream, outStream)
}

func TestWriteScanTwoIntegers(t *testing.T) {
	file := bytes.NewBuffer([]byte(""))
	description := src.StreamDescription{}.Add("col_1", src.ColumnInt).Add("col_2", src.ColumnInt)
	inStream := src.NewStream(description, [][]any{{int64(1), int64(2)}, {int64(3), int64(4)}, {int64(5), int64(6)}})

	err := src.Write(file, inStream)
	assert.NoError(t, err)

	outStream, err := src.Scan(file)
	assert.NoError(t, err)
	assert.Equal(t, inStream, outStream)
}

func TestWriteScanTextAndInteger(t *testing.T) {
	file := bytes.NewBuffer([]byte(""))
	description := src.StreamDescription{}.Add("col_1", src.ColumnText).Add("col_2", src.ColumnInt)
	inStream := src.NewStream(description, [][]any{{"a", int64(1)}, {"b", int64(2)}, {"fbd0b811-78e4-4c2f-b96d-0223818dc153", int64(3)}})

	err := src.Write(file, inStream)
	assert.NoError(t, err)

	outStream, err := src.Scan(file)
	assert.NoError(t, err)
	assert.Equal(t, inStream, outStream)
}

func TestSelectFields(t *testing.T) {
	file := bytes.NewBuffer([]byte(""))
	description := src.StreamDescription{}.Add("col_1", src.ColumnInt).Add("col_2", src.ColumnText).Add("col_3", src.ColumnInt)
	inStream := src.NewStream(description, [][]any{{int64(1), "a", int64(3)}, {int64(4), "b", int64(6)}, {int64(7), "fbd0b811-78e4-4c2f-b96d-0223818dc153", int64(9)}})

	err := src.Write(file, inStream)
	assert.NoError(t, err)

	outStream, err := src.Scan(file, "col_2")
	assert.NoError(t, err)
	assert.Equal(t, src.NewStream(
		src.StreamDescription{}.Add("col_2", src.ColumnText),
		[][]any{{"a"}, {"b"}, {"fbd0b811-78e4-4c2f-b96d-0223818dc153"}},
	), outStream)
}
