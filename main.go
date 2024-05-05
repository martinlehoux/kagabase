package main

import (
	"bytes"
	"os"

	"github.com/martinlehoux/kagabase/src"
)

const filename = "main_db"

func generate(size int) {
	file := new(bytes.Buffer)
	data := make([][]any, size)
	description := src.StreamDescription{}.Add("col_1", src.ColumnInt)
	for i := 0; i < size; i++ {
		data[i] = []any{int64(i)}
	}
	stream := src.NewStream(description, data)
	stream.Write(file)
	err := os.WriteFile(filename, file.Bytes(), 0644)
	if err != nil {
		panic(err)
	}
}

func benchmark() {
	file, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	reader := bytes.NewReader(file)
	reader.Seek(0, 0)
	src.Scan(reader)
}

func main() {
	benchmark()
}
