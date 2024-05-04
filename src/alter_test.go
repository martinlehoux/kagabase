package src_test

import (
	"testing"

	"github.com/martinlehoux/kagabase/src"
	"github.com/stretchr/testify/assert"
)

func TestAlterFrom1IntTo2Int(t *testing.T) {
	description := src.StreamDescription{}.Add("col_1", src.ColumnInt)
	inStream := src.NewStream(description, [][]any{{int64(1)}, {int64(2)}, {int64(3)}})

	outStream := inStream.AddColumn("col_2", src.ColumnInt)

	assert.Equal(t, src.NewStream(description.Add("col_2", src.ColumnInt), [][]any{{int64(1), int64(0)}, {int64(2), int64(0)}, {int64(3), int64(0)}}), outStream)
}
