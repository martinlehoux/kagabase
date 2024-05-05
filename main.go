package main

import (
	"bytes"
	"os"

	"github.com/martinlehoux/kagabase/src"
)

func main() {
	file, err := os.ReadFile("main_db")
	if err != nil {
		panic(err)
	}
	reader := bytes.NewReader(file)
	reader.Seek(0, 0)
	src.Scan(reader)
}
