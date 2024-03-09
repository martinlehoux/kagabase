profile:
	go test ./src -bench=. -benchmem -cpuprofile cpu.prof -memprofile mem.prof

bench:
	go test ./src -bench=. -benchmem -count=10 > bench.txt

compare:
	benchstat now.txt next.txt