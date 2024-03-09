profile:
	rm -f cpu.prof mem.prof src.test
	go test ./src -bench=. -benchmem -cpuprofile cpu.prof -memprofile mem.prof

bench:
	go test ./src -bench=. -benchmem -count=10 > bench.txt

compare:
	benchstat current.txt next.txt