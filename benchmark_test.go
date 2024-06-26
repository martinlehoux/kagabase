package main

import (
	"bytes"
	"testing"

	"github.com/martinlehoux/kagabase/src"
)

func BenchmarkScan1MRows1Integer(b *testing.B) {
	size := 1_000_000
	file := new(bytes.Buffer)
	data := make([][]any, size)
	description := src.StreamDescription{}.Add("col_1", src.ColumnInt)
	for i := 0; i < size; i++ {
		data[i] = []any{int64(i)}
	}
	stream := src.NewStream(description, data)
	stream.Write(file)
	b.ResetTimer()
	reader := bytes.NewReader(file.Bytes())

	for i := 0; i < b.N; i++ {
		reader.Seek(0, 0)
		src.SeqScan(reader)
	}
}
