`benchstat old.txt new.txt`

- go tool pprof -http=":" cpu.prof
- `go install golang.org/x/perf/cmd/benchstat@latest`

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
