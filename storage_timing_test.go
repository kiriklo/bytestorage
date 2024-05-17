package bytestorage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/cespare/xxhash/v2"
	"github.com/zeebo/xxh3"
)

// xxh3 and xxhash comparison

func BenchmarkXXHash_4byte(b *testing.B) {
	k := []byte("aabb")
	for i := 0; i < b.N; i++ {
		xxhash.Sum64(k)
	}
}

func BenchmarkXXH3_4byte(b *testing.B) {
	k := []byte("aabb")
	var s = uint64(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		xxh3.HashSeed(k, s)
	}
}
func BenchmarkXXHash_8byte(b *testing.B) {
	k := []byte("aabbccdd")
	for i := 0; i < b.N; i++ {
		xxhash.Sum64(k)
	}
}

func BenchmarkXXH3_8byte(b *testing.B) {
	k := []byte("aabbccdd")
	var s = uint64(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		xxh3.HashSeed(k, s)
	}
}
func BenchmarkXXHash_16byte(b *testing.B) {
	k := []byte("aabbccddaabbccdd")
	for i := 0; i < b.N; i++ {
		xxhash.Sum64(k)
	}
}
func BenchmarkXXH3_16byte(b *testing.B) {
	k := []byte("aabbccddaabbccdd")
	var s = uint64(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		xxh3.HashSeed(k, s)
	}
}
func BenchmarkXXHash_32byte(b *testing.B) {
	k := []byte("aabbccddaabbccddaabbccddaabbccdd")
	for i := 0; i < b.N; i++ {
		xxhash.Sum64(k)
	}
}
func BenchmarkXXH3_32byte(b *testing.B) {
	k := []byte("aabbccddaabbccddaabbccddaabbccdd")
	var s = uint64(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		xxh3.HashSeed(k, s)
	}
}
func BenchmarkXXHash_64byte(b *testing.B) {
	k := []byte("aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd")
	for i := 0; i < b.N; i++ {
		xxhash.Sum64(k)
	}
}
func BenchmarkXXH3_64byte(b *testing.B) {
	k := []byte("aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd")
	var s = uint64(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		xxh3.HashSeed(k, s)
	}
}
func BenchmarkXXHash_128byte(b *testing.B) {
	k := []byte("aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd")
	for i := 0; i < b.N; i++ {
		xxhash.Sum64(k)
	}
}
func BenchmarkXXH3_128byte(b *testing.B) {
	k := []byte("aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd")
	var s = uint64(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		xxh3.HashSeed(k, s)
	}
}

// Fast cache

func BenchmarkCacheSet(b *testing.B) {
	const items = 1 << 16
	c := fastcache.New(12 * items)
	defer c.Reset()
	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				c.Set(k, v)
			}
		}
	})
}

func BenchmarkCacheGet(b *testing.B) {
	const items = 1 << 16
	c := fastcache.New(12 * items)
	defer c.Reset()
	k := []byte("\x00\x00\x00\x00")
	v := []byte("xyza")
	for i := 0; i < items; i++ {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		c.Set(k, v)
	}

	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		var buf []byte
		k := []byte("\x00\x00\x00\x00")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				buf = c.Get(buf[:0], k)
				if string(buf) != string(v) {
					panic(fmt.Errorf("BUG: invalid value obtained; got %q; want %q", buf, v))
				}
			}
		}
	})
}

func BenchmarkCacheHas(b *testing.B) {
	const items = 1 << 16
	c := fastcache.New(12 * items)
	defer c.Reset()
	k := []byte("\x00\x00\x00\x00")
	for i := 0; i < items; i++ {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		c.Set(k, nil)
	}

	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				if !c.Has(k) {
					panic(fmt.Errorf("BUG: missing value for key %q", k))
				}
			}
		}
	})
}

func BenchmarkCacheSetGet(b *testing.B) {
	const items = 1 << 16
	c := fastcache.New(12 * items)
	defer c.Reset()
	b.ReportAllocs()
	b.SetBytes(2 * items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		var buf []byte
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				c.Set(k, v)
			}
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				buf = c.Get(buf[:0], k)
				if string(buf) != string(v) {
					panic(fmt.Errorf("BUG: invalid value obtained; got %q; want %q", buf, v))
				}
			}
		}
	})
}

// Byte storage

func BenchmarkBytestorageReplaceDiffrentValue(b *testing.B) {
	const items = 1 << 16
	key := []byte("xyza")
	val := []byte("\x00\x00\x00\x00")
	s := New()
	defer s.Reset()
	s.Set(key, val)
	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		k := key
		v := val
		for pb.Next() {
			for i := 0; i < items; i++ {
				v[0]++
				if v[0] == 0 {
					v[1]++
				}
				s.Set(k, v)
			}
		}
	})
}
func BenchmarkBytestorageReplaceSameValue(b *testing.B) {
	const items = 1 << 16
	k := []byte("xyza")
	v := []byte("\x00\x00\x00\x00")
	s := New()
	defer s.Reset()
	s.Set(k, v)
	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < items; i++ {
				s.Set(k, v)
			}
		}
	})
}

func BenchmarkBytestorageSet(b *testing.B) {
	const items = 1 << 16
	s := New()
	defer s.Reset()
	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				s.Set(k, v)
			}
		}
	})
}

func BenchmarkBytestorageGet(b *testing.B) {
	const items = 1 << 16
	s := New()
	defer s.Reset()
	k := []byte("\x00\x00\x00\x00")
	v := []byte("xyza")
	for i := 0; i < items; i++ {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		s.Set(k, v)
	}

	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		var buf []byte
		k := []byte("\x00\x00\x00\x00")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				buf = s.Get(buf[:0], k)
				if string(buf) != string(v) {
					panic(fmt.Errorf("BUG: invalid value obtained; got %q; want %q", buf, v))
				}
			}
		}
	})
}

func BenchmarkBytestorageHas(b *testing.B) {
	const items = 1 << 16
	c := New()
	defer c.Reset()
	k := []byte("\x00\x00\x00\x00")
	for i := 0; i < items; i++ {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		c.Set(k, nil)
	}

	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				if !c.Has(k) {
					panic(fmt.Errorf("BUG: missing value for key %q", k))
				}
			}
		}
	})
}

func BenchmarkBytestorageSetGet(b *testing.B) {
	const items = 1 << 16
	c := New()
	defer c.Reset()
	b.ReportAllocs()
	b.SetBytes(2 * items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		var buf []byte
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				c.Set(k, v)
			}
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				buf = c.Get(buf[:0], k)
				if string(buf) != string(v) {
					panic(fmt.Errorf("BUG: invalid value obtained; got %q; want %q", buf, v))
				}
			}
		}
	})
}

// Standart map

func BenchmarkStdMapSet(b *testing.B) {
	const items = 1 << 16
	m := make(map[string][]byte)
	var mu sync.Mutex
	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				mu.Lock()
				m[string(k)] = v
				mu.Unlock()
			}
		}
	})
}

func BenchmarkStdMapGet(b *testing.B) {
	const items = 1 << 16
	m := make(map[string][]byte)
	k := []byte("\x00\x00\x00\x00")
	v := []byte("xyza")
	for i := 0; i < items; i++ {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		m[string(k)] = v
	}

	var mu sync.RWMutex
	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				mu.RLock()
				vv := m[string(k)]
				mu.RUnlock()
				if string(vv) != string(v) {
					panic(fmt.Errorf("BUG: unexpected value; got %q; want %q", vv, v))
				}
			}
		}
	})
}

func BenchmarkStdMapSetGet(b *testing.B) {
	const items = 1 << 16
	m := make(map[string][]byte)
	var mu sync.RWMutex
	b.ReportAllocs()
	b.SetBytes(2 * items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				mu.Lock()
				m[string(k)] = v
				mu.Unlock()
			}
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				mu.RLock()
				vv := m[string(k)]
				mu.RUnlock()
				if string(vv) != string(v) {
					panic(fmt.Errorf("BUG: unexpected value; got %q; want %q", vv, v))
				}
			}
		}
	})
}

// Sync map

func BenchmarkSyncMapSet(b *testing.B) {
	const items = 1 << 16
	m := sync.Map{}
	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := "xyza"
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				m.Store(string(k), v)
			}
		}
	})
}

func BenchmarkSyncMapGet(b *testing.B) {
	const items = 1 << 16
	m := sync.Map{}
	k := []byte("\x00\x00\x00\x00")
	v := "xyza"
	for i := 0; i < items; i++ {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		m.Store(string(k), v)
	}

	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				vv, ok := m.Load(string(k))
				if !ok || vv.(string) != string(v) {
					panic(fmt.Errorf("BUG: unexpected value; got %q; want %q", vv, v))
				}
			}
		}
	})
}

func BenchmarkSyncMapSetGet(b *testing.B) {
	const items = 1 << 16
	m := sync.Map{}
	b.ReportAllocs()
	b.SetBytes(2 * items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := "xyza"
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				m.Store(string(k), v)
			}
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				vv, ok := m.Load(string(k))
				if !ok || vv.(string) != string(v) {
					panic(fmt.Errorf("BUG: unexpected value; got %q; want %q", vv, v))
				}
			}
		}
	})
}
