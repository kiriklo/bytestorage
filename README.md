# bytestorage - fast thread-safe inmemory storage in Go

### Features

* Fast. Performance scales on multi-core CPUs. See benchmark results below.
* Thread-safe. Concurrent goroutines may read and write into a single
  storage instance.
* Hash collision handling using [open addressing](https://en.wikipedia.org/wiki/Open_addressing).
* Simple source code.

### Benchmarks

`Bytestorage` performance is compared with [Fastcache](https://github.com/VictoriaMetrics/fastcache), standard Go map
and [sync.Map](https://golang.org/pkg/sync/#Map).

```
$ GOMAXPROCS=4 go test -bench='Set|Get' -benchtime=10s
goos: windows
goarch: amd64
pkg: byte-storage
cpu: 11th Gen Intel(R) Core(TM) i9-11900K @ 3.50GHz
BenchmarkCacheSet-4                 5176           2243610 ns/op          29.21 MB/s        7572 B/op          2 allocs/op
BenchmarkCacheGet-4                 7718           1671282 ns/op          39.21 MB/s        5076 B/op          1 allocs/op
BenchmarkCacheSetGet-4              2875           4464686 ns/op          29.36 MB/s       13635 B/op          3 allocs/op
BenchmarkBytestorageSet-4           7032           1586558 ns/op          41.31 MB/s        2310 B/op         22 allocs/op
BenchmarkBytestorageGet-4           7762           1551915 ns/op          42.23 MB/s        2093 B/op         20 allocs/op
BenchmarkBytestorageSetGet-4        3754           3239362 ns/op          40.46 MB/s        4325 B/op         42 allocs/op
BenchmarkStdMapSet-4                1509           7793608 ns/op           8.41 MB/s      270256 B/op      65537 allocs/op
BenchmarkStdMapGet-4                7447           1531113 ns/op          42.80 MB/s        1677 B/op          8 allocs/op
BenchmarkStdMapSetGet-4              403          33168520 ns/op           3.95 MB/s      292469 B/op      65539 allocs/op
BenchmarkSyncMapSet-4                841          13997539 ns/op           4.68 MB/s     3418756 B/op     262301 allocs/op
BenchmarkSyncMapGet-4              14728            809663 ns/op          80.94 MB/s         851 B/op         26 allocs/op
BenchmarkSyncMapSetGet-4            2436           5156542 ns/op          25.42 MB/s     3411625 B/op     262198 allocs/op
```

`MB/s` column here actually means `millions of operations per second`.

### Limitations

* Keys and values must be byte slices. Other types must be marshaled before
  storing them in the cache.
