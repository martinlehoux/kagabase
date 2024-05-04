package src

func (in *Stream) AddColumn(name string, t ColumnType) Stream {
	out := NewStream(in.description.Add(name, t), [][]any{})
	for _, row := range in.Values {
		out.Values = append(out.Values, append(row, t.Default()))
	}
	return out
}
