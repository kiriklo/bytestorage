package bytestorage

import (
	"bytes"
	"github.com/zeebo/xxh3"
	"sync"
	"sync/atomic"
)

const (
	// Number of buckets in storage (same as fastcache).
	bucketsCount = 512

	// Byte length of key/value to pre-allocate in bucket.
	entriesSize = 8

	// Number of elements in each bucket to pre-allocate.
	entriesCount = 16

	// Number of deleted elements to pre-allocate in bucket.
	freeSize = 2
)

// ---- High level ----

// Stats represents storage stats.
//
// Use UpdateStats for obtaining fresh stats from the storage.
type Stats struct {
	// GetCalls is the number of Get calls.
	GetCalls uint64

	// SetCalls is the number of Set calls.
	SetCalls uint64

	// Misses is the number of storage misses.
	Misses uint64

	// Collisions is the number of hash collisions.
	Collisions uint64

	// EntriesCount is the current number of entries in the storage.
	EntriesCount uint64

	// BytesSize is the current size of the storage in bytes.
	BytesSize uint64
}

// Reset resets s, so it may be re-used again in Storage.UpdateStats.
func (s *Stats) Reset() {
	*s = Stats{}
}

// Storage just contains an array of buckets
type Storage struct {
	buckets [bucketsCount]bucket
}

// New returns new Storage.
func New() *Storage {
	var s Storage
	for i := range s.buckets[:] {
		s.buckets[i].init()
	}
	return &s
}

// Reset removes all the items from the storage.
func (s *Storage) Reset() {
	for i := range s.buckets[:] {
		s.buckets[i].reset()
		s.buckets[i].init()
	}
}

// UpdateStats adds storage stats to s.
//
// Call s.Reset before calling UpdateStats if s is re-used.
func (s *Storage) UpdateStats(stats *Stats) {
	for i := range s.buckets[:] {
		s.buckets[i].updateStats(stats)
	}
}

// Set stores (k, v) in the storage.
// nil key is acceptable.
func (s *Storage) Set(k, v []byte) {
	h := xxh3.Hash(k)
	idx := h % bucketsCount
	s.buckets[idx].set(k, v, h)
}

// Get returns value for the given key k.
func (s *Storage) Get(dst, k []byte) []byte {
	h := xxh3.Hash(k)
	idx := h % bucketsCount
	dst, _ = s.buckets[idx].get(dst, k, h)
	return dst
}

// HasGet works identically to Get, but also returns whether the given key
// exists in the storage.
func (s *Storage) HasGet(dst, k []byte) ([]byte, bool) {
	h := xxh3.Hash(k)
	idx := h % bucketsCount
	return s.buckets[idx].get(dst, k, h)
}

// Has returns true if entry for the given key k exists in the storage.
func (s *Storage) Has(k []byte) bool {
	h := xxh3.Hash(k)
	idx := h % bucketsCount
	return s.buckets[idx].has(k, h)
}

// Del deletes value for the given k from the storage.
func (s *Storage) Del(k []byte) {
	h := xxh3.Hash(k)
	idx := h % bucketsCount
	s.buckets[idx].del(k, h)
}

// Size returns storage size.
//
// Prefer using Storage.UpdateStats
func (s *Storage) Size() uint64 {
	var size uint64
	for i := range s.buckets[:] {
		size += atomic.LoadUint64(&s.buckets[i].size)
	}
	return size
}

// Collision returns storage collision.
//
// Prefer using Storage.UpdateStats
func (s *Storage) Collision() uint64 {
	var col uint64
	for i := range s.buckets[:] {
		col += atomic.LoadUint64(&s.buckets[i].collisions)
	}
	return col
}

// EntriesCount returns number of entries in storage.
//
// Prefer using Storage.UpdateStats
func (s *Storage) EntriesCount() uint64 {
	var c uint64
	for i := range s.buckets[:] {
		c += s.buckets[i].getEntriesCount()
	}
	return c
}

//    ---- Low level ----
//
//    Bucket visualisation:
//
//           m map                                    col map
//    ------------------                         ---------------------
//    |  hash  |  idx  |                         |  hash  |  []idx   |
//    ------------------                         ---------------------
//    |  hash1 |   0   |-----------              |  hash3 |  2, 3    |-----------
//    |  hash2 |   1   |--------  |              |  hash4 |  4, 5, 6 |          |
//    |  ...   |  ...  |       |  |              |  ...   |  ...     |          |
//    |  hashN |  N-1  |-----  |  |              |  hashN |  N...    |          |
//    ------------------    |  |  |              ---------------------          |
//                          |  |  |                                             |
//                          |  |  |                 kv 3d slice                 |
//                          |  |  |     * * * * * * * * * * * * * * * * * *     |
//                          |  |  |     *               key1              *     |
//                          |  |  |     *           |---[0]--->[]byte     *     |
//                          |  |  |     * hash1     |                     *     |
//                          |  |  ------> [0]--------                     *     |
//                          |  |        *           |  value1             *     |
//                          |  |        *           |---[1]--->[]byte     *     |
//                          |  |        *                                 *     |
//                          |  |        * * * * * * * * * * * * * * * * * *     |
//                          |  |        *               key2              *     |
//                          |  |        *           |---[0]--->[]byte     *     |
//                          |  |        * hash2     |                     *     |
//                          |  ---------> [1]--------                     *     |
//                          |           *           |  value2             *     |
//                          |           *           |---[1]--->[]byte     *     |
//                          |           *                                 *     |
//                          |           * * * * * * * * * * * * * * * * * *     |
//                          |           *               key3              *     |
//                          |           *    []byte <---[0]---|           *     |
//                          |           *                     |     hash3 *     |
//                          |           *                     --------[2] <-----|
//                          |           *              value3 |           *     |
//                          |           *    []byte <---[1]---|           *     |
//                          |           *                                 *     |
//                          |           * * * * * * * * * * * * * * * * * *     |
//                          |           *               key4              *     |
//                          |           *    []byte <---[0]---|           *     |
//                          |           *                     |     hash3 *     |
//                          |           *                     --------[3] <-----|
//                          |           *              value4 |           *
//                          |           *    []byte <---[1]---|           *
//                          |           *                                 *
//                          |           * * * * * * * * * * * * * * * * * *
//                          |           *                                 *
//                          |           *               ...               *
//                          |           *                                 *
//                          |           * * * * * * * * * * * * * * * * * *
//                          |           *               keyN              *
//                          |           *           |---[0]--->[]byte     *
//                          |           * hashN     |                     *
//                          ------------> [N-1]------                     *
//                                      *           |  valueN             *
//                                      *           |---[1]--->[]byte     *
//                                      *                                 *
//                                      * * * * * * * * * * * * * * * * * *

type bucket struct {
	mu sync.RWMutex

	// Bucket size
	size uint64

	// m maps hash(k) to idx of (k, v) pair in kv.
	m map[uint64]uint64

	// col represents map with hash(k) to multiple keys having same hash
	col map[uint64][]uint64

	// Main key-value byte slice. Both m and col shares it.
	kv [][2][]byte

	// Contains deleted entires in kv
	free []uint64

	// Bucket offset shows the position of last entry in the kv.
	offset uint64

	getCalls   uint64
	setCalls   uint64
	misses     uint64
	collisions uint64
}

func (b *bucket) init() {
	b.mu.Lock()
	b.m = make(map[uint64]uint64, entriesCount)
	b.kv = make([][2][]byte, entriesCount)
	b.col = make(map[uint64][]uint64) // no need to pre-allocate, considering collision unlikely to happen
	b.free = make([]uint64, 0, freeSize)
	for i := 0; i < entriesCount; i++ {
		b.kv[i][0] = make([]byte, 0, entriesSize)
		b.kv[i][1] = make([]byte, 0, entriesSize)
	}
	b.mu.Unlock()
}

func (b *bucket) reset() {
	b.mu.Lock()
	atomic.StoreUint64(&b.size, 0)
	clear(b.m)
	clear(b.col)
	clear(b.kv)
	clear(b.free)
	b.offset = 0
	atomic.StoreUint64(&b.getCalls, 0)
	atomic.StoreUint64(&b.setCalls, 0)
	atomic.StoreUint64(&b.misses, 0)
	atomic.StoreUint64(&b.collisions, 0)
	b.mu.Unlock()
}

func (b *bucket) updateStats(s *Stats) {
	s.GetCalls += atomic.LoadUint64(&b.getCalls)
	s.SetCalls += atomic.LoadUint64(&b.setCalls)
	s.Misses += atomic.LoadUint64(&b.misses)
	s.Collisions += atomic.LoadUint64(&b.collisions)
	s.BytesSize += atomic.LoadUint64(&b.size)

	b.mu.RLock()
	s.EntriesCount += uint64(len(b.kv))
	for _, v := range b.col {
		s.EntriesCount += uint64(len(v))
	}

	b.mu.RUnlock()
}

func (b *bucket) getEntriesCount() (size uint64) {
	b.mu.RLock()
	size += uint64(len(b.m))
	for _, v := range b.col {
		size += uint64(len(v))
	}
	b.mu.RUnlock()
	return
}

func (b *bucket) get(dst, k []byte, h uint64) ([]byte, bool) {
	atomic.AddUint64(&b.getCalls, 1)
	var found bool
	var idx uint64
	var idxs []uint64
	// Collision protection
	b.mu.RLock()
	if atomic.LoadUint64(&b.collisions) != 0 {
		// Check if hash is in collision map
		idxs, found = b.col[h]
		// Hash is in col
		if found {
			atomic.AddUint64(&b.collisions, 1)
			for _, idx = range idxs {
				// Key exist in kv
				if string(b.kv[idx][0]) == string(k) {
					dst = append(dst, b.kv[idx][1]...)
					goto end
				}
			}
			// Hash exist in col but could not find the given k
			atomic.AddUint64(&b.misses, 1)
			goto end
		}
		// No collision for this hash, continue to search in m
		goto mcheck
	}
mcheck:
	idx, found = b.m[h]
	if found {
		if string(b.kv[idx][0]) == string(k) {
			dst = append(dst, b.kv[idx][1]...)
			goto end
		}
		atomic.AddUint64(&b.collisions, 1)
	}
	atomic.AddUint64(&b.misses, 1)
end:
	b.mu.RUnlock()
	return dst, found
}

// Need for compatability with fastcache
func (b *bucket) has(k []byte, h uint64) bool {
	atomic.AddUint64(&b.getCalls, 1)
	var found bool
	var idx uint64
	var idxs []uint64
	b.mu.RLock()
	if atomic.LoadUint64(&b.collisions) != 0 {
		idxs, found = b.col[h]
		if found {
			atomic.AddUint64(&b.collisions, 1)
			for _, idx = range idxs {
				if string(b.kv[idx][0]) == string(k) {
					goto end
				}
			}
			// Hash exist in col but could not find the given k
			atomic.AddUint64(&b.misses, 1)
			found = false
			goto end
		}
		// No collision for this hash, continue to search in m
		goto mcheck
	}
mcheck:
	idx, found = b.m[h]
	if found {
		if string(b.kv[idx][0]) == string(k) {
			goto end
		}
		found = false
		atomic.AddUint64(&b.collisions, 1)
	}
	atomic.AddUint64(&b.misses, 1)
end:
	b.mu.RUnlock()
	return found
}

func (b *bucket) set(k, v []byte, h uint64) {
	atomic.AddUint64(&b.setCalls, 1)
	var found bool
	var idx uint64
	var idxs []uint64

	// It's slow to check the col every time to see if it conains the hash.
	// Because collision is unlikely to happen we can just check if b.collisions
	// is null instaed of locking collision map (col) every time we call set/get.
	b.mu.Lock()
	if atomic.LoadUint64(&b.collisions) != 0 {
		// Check if given hash exist in collision map
		idxs, found = b.col[h]
		if found {
			b.collisions++
			// Iterating through keys
			for _, idx = range idxs {
				if string(b.kv[idx][0]) == string(k) {
					// Value is the same. Nothing to do...
					if string(b.kv[idx][1]) == string(v) {
						goto end
					}

					// Split into 2 separate uint64 to avoid uint64 overflow in case
					// length of new value in smaller then length of value in kv
					atomic.AddUint64(&b.size, uint64(len(v))-uint64(len(b.kv[idx][1])))
					//b.size += uint64(len(v)) - uint64(len(b.kv[idx][1]))

					// Slice has enough capacity to contain v
					if cap(b.kv[idx][1]) >= len(v) {
						b.kv[idx][1] = b.kv[idx][1][:len(v)]
						copy(b.kv[idx][1], v)
					} else {
						b.kv[idx][1] = bytes.Clone(v)
					}
					goto end
				}
			}
			// New key has same hash, add new key
			idxs = append(idxs, b.offset)
			b.col[h] = idxs
			goto add
		}
		goto mcheck
	}
mcheck:
	// Check if key already exist in map
	idx, found = b.m[h]
	if found {
		// Second collision check
		if string(b.kv[idx][0]) != string(k) {
			// Found a new pair of keys that has the same hash
			atomic.AddUint64(&b.collisions, 1)
			//b.collisions++
			newIdxs := make([]uint64, 2)
			// Add old key idx
			newIdxs[0] = idx
			// Add new key idx
			newIdxs[1] = b.offset
			// Add hash with keys to collision map
			b.col[h] = newIdxs
			// Remove hash from m
			delete(b.m, h)
			goto add
		}
		// Value is the same. Nothing to do...
		if string(b.kv[idx][1]) == string(v) {
			goto end
		}

		atomic.AddUint64(&b.size, uint64(len(v))-uint64(len(b.kv[idx][1])))
		//b.size += uint64(len(v)) - uint64(len(b.kv[idx][1]))
		// Slice has enough capacity to contain v
		if cap(b.kv[idx][1]) >= len(v) {
			b.kv[idx][1] = b.kv[idx][1][:len(v)]
			copy(b.kv[idx][1], v)
		} else {
			b.kv[idx][1] = bytes.Clone(v)
		}
		goto end
	}
	// Check if free space exist
	if l := len(b.free); l > 0 {
		idx = b.free[l-1]
		atomic.AddUint64(&b.size, uint64(len(v)+len(k)))

		// Slice has enough capacity to contain key
		if cap(b.kv[idx][0]) >= len(k) {
			b.kv[idx][0] = b.kv[idx][0][:len(k)]
			copy(b.kv[idx][0], k)
		} else {
			b.kv[idx][0] = bytes.Clone(k)
		}

		// Slice has enough capacity to contain value
		if cap(b.kv[idx][1]) >= len(v) {
			b.kv[idx][1] = b.kv[idx][1][:len(v)]
			copy(b.kv[idx][1], v)
		} else {
			b.kv[idx][1] = bytes.Clone(v)
		}

		// Remove last item from free slice
		b.free = b.free[:l-1]
		b.m[h] = idx
		goto end
	}

	// Key do not exist in map and no deleted items
	b.m[h] = b.offset

add:
	// kv has free space to store one more element
	if b.offset < uint64(len(b.kv)) {
		// Slice has enough capacity to contain key
		if cap(b.kv[b.offset][0]) >= len(k) {
			b.kv[b.offset][0] = b.kv[b.offset][0][:len(k)]
			copy(b.kv[b.offset][0], k)
		} else {
			b.kv[b.offset][0] = bytes.Clone(k)
		}

		// Slice has enough capacity to contain value
		if cap(b.kv[b.offset][1]) >= len(v) {
			b.kv[b.offset][1] = b.kv[b.offset][1][:len(v)]
			copy(b.kv[b.offset][1], v)
		} else {
			b.kv[b.offset][1] = bytes.Clone(v)
		}

	} else {
		// If not, append to kv
		newKv := [2][]byte{}
		newKv[0] = bytes.Clone(k)
		newKv[1] = bytes.Clone(v)
		b.kv = append(b.kv, newKv)
	}
	b.offset++
	atomic.AddUint64(&b.size, uint64(len(v)+len(k)))
end:
	b.mu.Unlock()
}

func (b *bucket) del(k []byte, h uint64) {
	var found bool
	var idx uint64
	var pos int
	var idxs []uint64
	b.mu.Lock()
	if atomic.LoadUint64(&b.collisions) != 0 {
		// Check if hash is in collision map
		idxs, found = b.col[h]
		// Hash is in col
		if found {
			atomic.AddUint64(&b.collisions, 1)
			for pos, idx = range idxs {
				// Key exist in kv
				if string(b.kv[idx][0]) == string(k) {
					atomic.AddUint64(&b.size, -uint64(len(b.kv[idx][0])+len(b.kv[idx][1])))
					//b.size -= uint64(len(b.kv[idx][0]) + len(b.kv[idx][1]))

					// Clear kv[i] but keep allocated memory
					b.kv[idx][0] = b.kv[idx][0][0:0]
					b.kv[idx][1] = b.kv[idx][1][0:0]

					// Add deleted element to free slice
					b.free = append(b.free, uint64(idx))

					// Idxs order is not important so delete without preserving order.
					idxs[pos] = idxs[len(idxs)-1]
					idxs = idxs[:len(idxs)-1]

					// Then are more than 2 keys in storage that have same hash
					// after removing one, it means that there are still key that causes hash collisions.
					// So just remove one key from idxs and update the collision map
					if len(idxs) >= 2 {
						b.col[h] = idxs
						goto end
					}
					// There are 2 keys that causes hash collision. So after removing one of them
					// collision will not exists any more. Remove this hash from collision map and
					// add it to m
					if len(idxs) != 1 {
						panic("BUG: idxs size is not 1.")
					}

					b.m[h] = idxs[0]
					delete(b.col, h)
					goto end
				}
			}
			// Hash exist in col but could not find the given k
			atomic.AddUint64(&b.misses, 1)
			goto end
		}
		goto mcheck
	}
mcheck:
	idx, found = b.m[h]
	if !found {
		goto end
	}
	if string(b.kv[idx][0]) == string(k) {
		atomic.AddUint64(&b.size, -uint64(len(b.kv[idx][0])+len(b.kv[idx][1])))
		//b.size -= uint64(len(b.kv[idx][0]) + len(b.kv[idx][1]))
		b.kv[idx][0] = b.kv[idx][0][0:0]
		b.kv[idx][1] = b.kv[idx][1][0:0]
		b.free = append(b.free, idx)
		delete(b.m, h)
		goto end
	}
	atomic.AddUint64(&b.collisions, 1)
end:
	b.mu.Unlock()
}
