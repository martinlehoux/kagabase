- For profiling: `go test -bench=. -benchmem -memprofile mem.out -cpuprofile cpu.out`
- For comparison

```sh
go test -bench=. -benchmem -count=5
benchstat old.txt new.txt
```
