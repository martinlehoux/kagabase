package src_test

import (
	"bytes"
	"testing"

	"github.com/martinlehoux/kagabase/src"
	"github.com/stretchr/testify/assert"
)

func TestTableDescription(t *testing.T) {
	description := src.TableDescription{}.Add("col_1", src.ColumnInt).Add("col_2", src.ColumnText)
	encoded := description.Encode()
	decoded, err := src.DecodeDescription(bytes.NewReader(encoded))
	assert.NoError(t, err)
	assert.Equal(t, description, decoded)
}
