package src_test

import (
	"bytes"
	"testing"

	"github.com/martinlehoux/kagabase/src"
	"github.com/stretchr/testify/assert"
)

func TestStreamDescription(t *testing.T) {
	description := src.StreamDescription{}.Add("col_1", src.ColumnInt).Add("col_2", src.ColumnText)
	encoded := description.Encode()
	decoded, err := src.DecodeStreamDescription(bytes.NewReader(encoded))
	assert.NoError(t, err)
	assert.Equal(t, description, decoded)
}
