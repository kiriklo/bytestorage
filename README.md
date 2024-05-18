[![Build Status](https://github.com/kiriklo/bytestorage/workflows/main/badge.svg)](https://github.com/kiriklo/bytestorage/actions)
[![codecov](https://codecov.io/gh/kiriklo/bytestorage/branch/main/graph/badge.svg)](https://codecov.io/gh/kiriklo/bytestorage/)
# bytestorage - fast thread-safe inmemory storage in Go

### Features

* Fast. Performance scales on multi-core CPUs. See benchmark results below.
* Thread-safe. Concurrent goroutines may read and write into a single
  storage instance.
* Hash collision handling using [open addressing](https://en.wikipedia.org/wiki/Open_addressing).
* Simple source code, no unsafe pointers or byte shifting.
* No limitations on key/value size

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
BenchmarkBytestorageSet-4           7137           1555707 ns/op          42.13 MB/s        2275 B/op         22 allocs/op
BenchmarkBytestorageGet-4           7768           1361895 ns/op          48.12 MB/s        2092 B/op         20 allocs/op
BenchmarkBytestorageSetGet-4        3832           3066633 ns/op          42.74 MB/s        4238 B/op         42 allocs/op
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
* You should think about bytestorage as sync map rather then cache. Bytestorage
  has no overflow, so you should control it's size.

### Architecture details

Bytestorage is based on [Fastcache](https://github.com/VictoriaMetrics/fastcache). It has similar api, but
bucket structure is a bit diffrent.

* The cache consists of many buckets, each with its own lock.
  This helps scaling the performance on multi-core CPUs, since multiple
  CPUs may concurrently access distinct buckets.
* Each bucket consists of a `hash(key) -> (key, value) position` map, 
  `hash(key) -> [](key, value) positions` collision map and 3d
  byte slice holding `(key, value)` entries.