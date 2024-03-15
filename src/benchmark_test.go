package src_test

import (
	"bytes"
	"testing"

	"github.com/martinlehoux/kagabase/src"
)

func BenchmarkScan1MRows1Integer(b *testing.B) {
	size := 1_000_000
	file := bytes.NewBuffer([]byte(""))
	description := src.StreamDescription{}.Add("col_1", src.ColumnInt)
	data := make([][]any, size)
	for i := 0; i < size; i++ {
		data = append(data, []any{int64(i)})
	}
	stream := src.NewStream(description, data)
	src.Write(file, stream)
	b.ResetTimer()
	reader := bytes.NewReader(file.Bytes())

	for i := 0; i < b.N; i++ {
		reader.Seek(0, 0)
		src.Scan(reader)
	}
}
