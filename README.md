## Benchmarking

- `go install golang.org/x/perf/cmd/benchstat@latest`
- `go test -bench=. -count=10 > next.txt`
- `benchstat current.txt next.txt`
- go tool pprof -http=":" cpu.prof

## Migration

- Add version in table to upgrade

## IO

- How to read & write concurrently ?
- How to insert ?

## Perf

- Avoid type casting and use unsafe

## Interface

```
result = []
for (a,c) in table:
  result += (a,c)
return result
```
