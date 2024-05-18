package bytestorage

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
	//"github.com/cespare/xxhash/v2"
	//"github.com/zeebo/xxh3"
	// "golang.org/x/exp/rand"
)

const (
	brokenHash  uint64 = 1234567890
	brokenHash2 uint64 = 987654321
)

// For testing purpose only!!!
// Generates same hash for every key

func (s *Storage) colSet(k, v []byte, h uint64) {
	idx := h % bucketsCount
	s.buckets[idx].set(k, v, h)
}
func (s *Storage) colGet(dst, k []byte, h uint64) []byte {
	idx := h % bucketsCount
	dst, _ = s.buckets[idx].get(dst, k, h)
	return dst
}
func (s *Storage) colDel(k []byte, h uint64) {
	idx := h % bucketsCount
	s.buckets[idx].del(k, h)
}
func (s *Storage) colHas(k []byte, h uint64) bool {
	idx := h % bucketsCount
	return s.buckets[idx].has(k, h)
}

// func (s *Storage) debug() {
// 	for i := range s.buckets[:] {
// 		s.buckets[i].debug()
// 	}
// }
// func (b *bucket) debug() {
// 	b.mu.RLock()
// 	if len(b.col) > 0 {
// 		fmt.Println("Collision map:", b.col)
// 		fmt.Println("kv:", b.kv)

// 	}
// 	if len(b.m) > 0 {
// 		fmt.Println("Map:", b.m)
// 		fmt.Println("kv:", b.kv)
// 	}
// 	if int(b.offset) > 0 {
// 		fmt.Println("Offset:", b.offset)
// 	}
// 	b.mu.RUnlock()
// }

// Expecting that hash function will always return the same hash for the same key.
func TestStorageCollision(t *testing.T) {
	c := New()
	defer c.Reset()

	// Add first key with constant hash
	c.colSet([]byte("key"), []byte("value"), brokenHash)
	if v := c.colGet(nil, []byte("key"), brokenHash); string(v) != "value" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "value")
	}
	if s := c.EntriesCount(); s != uint64(1) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 1)
	}
	if s := c.Size(); s != uint64(8) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 8)
	}
	if col := c.Collision(); col != 0 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 1)
	}

	// Add second key with constant hash
	c.colSet([]byte("aaa"), []byte("bbb"), brokenHash)                     // +1 col
	if v := c.colGet(nil, []byte("aaa"), brokenHash); string(v) != "bbb" { // +1 col
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if s := c.EntriesCount(); s != uint64(2) {
		t.Fatalf("unexpected entries count  obtained; got %d; want %d", s, 2)
	}
	if s := c.Size(); s != uint64(14) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 14)
	}
	if col := c.Collision(); col != 2 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 2)
	}

	// Check that first key is there
	if v := c.colGet(nil, []byte("key"), brokenHash); string(v) != "value" { // +1 col
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "value")
	}
	if s := c.EntriesCount(); s != uint64(2) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 2)
	}
	if s := c.Size(); s != uint64(14) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 14)
	}

	if col := c.Collision(); col != 3 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 3)
	}

	// Add 'normal' key
	c.Set([]byte("bbb"), []byte("ccc"))
	if v := c.Get(nil, []byte("bbb")); string(v) != "ccc" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ccc")
	}
	if s := c.EntriesCount(); s != uint64(3) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 2)
	}
	if s := c.Size(); s != uint64(20) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 20)
	}
	if col := c.Collision(); col != 3 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 3)
	}

	// Add third key with constant hash
	c.colSet([]byte("ccc"), []byte("ddd"), brokenHash)                     // +1 col
	if v := c.colGet(nil, []byte("ccc"), brokenHash); string(v) != "ddd" { // +1 col
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ddd")
	}
	if s := c.EntriesCount(); s != uint64(4) {
		t.Fatalf("unexpected entries count  obtained; got %d; want %d", s, 4)
	}
	if s := c.Size(); s != uint64(26) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 26)
	}
	if col := c.Collision(); col != 5 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 5)
	}

	// Check that first key is there
	if v := c.colGet(nil, []byte("key"), brokenHash); string(v) != "value" { // +1 col
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "value")
	}
	// Check that second key is there
	if v := c.colGet(nil, []byte("aaa"), brokenHash); string(v) != "bbb" { // +1 col
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}

	// Check that 'normal' key is there
	if v := c.Get(nil, []byte("bbb")); string(v) != "ccc" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ccc")
	}
	if col := c.Collision(); col != 7 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 7)
	}

	// Delete third key with constant hash
	c.colDel([]byte("ccc"), brokenHash)                          // +1 col
	if v := c.colGet(nil, []byte("ccc"), brokenHash); v != nil { // +1 col
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if s := c.EntriesCount(); s != uint64(3) {
		t.Fatalf("unexpected entries count  obtained; got %d; want %d", s, 3)
	}
	if s := c.Size(); s != uint64(20) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 20)
	}
	if col := c.Collision(); col != 9 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 9)
	}
	// Check that first key is there
	if v := c.colGet(nil, []byte("key"), brokenHash); string(v) != "value" { // +1 col
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "value")
	}
	// Check that second key is there
	if v := c.colGet(nil, []byte("aaa"), brokenHash); string(v) != "bbb" { // +1 col
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}

	// Check that 'normal' key is there
	if v := c.Get(nil, []byte("bbb")); string(v) != "ccc" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ccc")
	}
	if col := c.Collision(); col != 11 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 11)
	}

	// Delete 'normal' key with constant hash
	c.Del([]byte("bbb"))
	if v := c.Get(nil, []byte("bbb")); v != nil {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if s := c.EntriesCount(); s != uint64(2) {
		t.Fatalf("unexpected entries count  obtained; got %d; want %d", s, 2)
	}
	if s := c.Size(); s != uint64(14) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 14)
	}
	if col := c.Collision(); col != 11 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 11)
	}

	// Check that first key is there
	if v := c.colGet(nil, []byte("key"), brokenHash); string(v) != "value" { // +1 col
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "value")
	}
	// Check that second key is there
	if v := c.colGet(nil, []byte("aaa"), brokenHash); string(v) != "bbb" { // +1 col
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if col := c.Collision(); col != 13 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 13)
	}

	// Delete first key with constant hash
	c.colDel([]byte("key"), brokenHash)                          // +1 col
	if v := c.colGet(nil, []byte("key"), brokenHash); v != nil { // +1 col
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if s := c.EntriesCount(); s != uint64(1) {
		t.Fatalf("unexpected entries count  obtained; got %d; want %d", s, 1)
	}
	if s := c.Size(); s != uint64(6) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}
	if col := c.Collision(); col != 15 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 15)
	}

	// Check that second key is there
	if v := c.colGet(nil, []byte("aaa"), brokenHash); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if col := c.Collision(); col != 15 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 15)
	}

	// Delete unknown key with constant hash
	c.colDel([]byte("new key"), brokenHash) // +1 col
	if v := c.colGet(nil, []byte("aaa"), brokenHash); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if s := c.EntriesCount(); s != uint64(1) {
		t.Fatalf("unexpected entries count  obtained; got %d; want %d", s, 1)
	}
	if s := c.Size(); s != uint64(6) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}
	if col := c.Collision(); col != 16 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 16)
	}

	if v := c.colGet(nil, []byte("bbb"), brokenHash); string(v) != "" { // +1 col
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if col := c.Collision(); col != 17 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 17)
	}

	// Add again third key with constant hash
	c.colSet([]byte("ccc"), []byte("ddd"), brokenHash)                     // +1 col
	if v := c.colGet(nil, []byte("ccc"), brokenHash); string(v) != "ddd" { // +1 col
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ddd")
	}
	if s := c.EntriesCount(); s != uint64(2) {
		t.Fatalf("unexpected entries count  obtained; got %d; want %d", s, 2)
	}
	if s := c.Size(); s != uint64(12) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 12)
	}
	if col := c.Collision(); col != 19 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 19)
	}

	c.Reset()
	// Add first key with constant hash 1
	c.colSet([]byte("aaa"), []byte("bbb"), brokenHash)
	if v := c.colGet(nil, []byte("aaa"), brokenHash); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if s := c.EntriesCount(); s != uint64(1) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 1)
	}
	if s := c.Size(); s != uint64(6) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}
	if col := c.Collision(); col != 0 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 0)
	}
	// Add second key with constant hash 1
	c.colSet([]byte("bbb"), []byte("ccc"), brokenHash)
	if v := c.colGet(nil, []byte("bbb"), brokenHash); string(v) != "ccc" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ccc")
	}
	if s := c.EntriesCount(); s != uint64(2) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 2)
	}
	if s := c.Size(); s != uint64(12) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 12)
	}
	if col := c.Collision(); col != 2 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 2)
	}

	// Add first key with constant hash 2
	c.colSet([]byte("ccc"), []byte("ddd"), brokenHash2)
	if v := c.colGet(nil, []byte("ccc"), brokenHash2); string(v) != "ddd" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ddd")
	}
	if s := c.EntriesCount(); s != uint64(3) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 3)
	}
	if s := c.Size(); s != uint64(18) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 18)
	}
	if col := c.Collision(); col != 2 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 2)
	}

	// Add second key with constant hash 2
	c.colSet([]byte("ddd"), []byte("eee"), brokenHash2)
	if v := c.colGet(nil, []byte("ddd"), brokenHash2); string(v) != "eee" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "eee")
	}
	if s := c.EntriesCount(); s != uint64(4) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 4)
	}
	if s := c.Size(); s != uint64(24) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 24)
	}
	if col := c.Collision(); col != 4 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 4)
	}

	// Add 'normal' key
	c.Set([]byte("eee"), []byte("fff"))
	if v := c.Get(nil, []byte("eee")); string(v) != "fff" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "fff")
	}
	if s := c.EntriesCount(); s != uint64(5) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 5)
	}
	if s := c.Size(); s != uint64(30) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 30)
	}
	if col := c.Collision(); col != 4 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 4)
	}

	// Check if all keys are there
	if v := c.colGet(nil, []byte("aaa"), brokenHash); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v := c.colGet(nil, []byte("bbb"), brokenHash); string(v) != "ccc" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ccc")
	}
	if v := c.colGet(nil, []byte("ccc"), brokenHash2); string(v) != "ddd" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ddd")
	}
	if v := c.colGet(nil, []byte("ddd"), brokenHash2); string(v) != "eee" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "eee")
	}
	if col := c.Collision(); col != 8 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 8)
	}

	// Replace all values
	c.colSet([]byte("aaa"), []byte("bbbbbbbbbb"), brokenHash)
	if v := c.colGet(nil, []byte("aaa"), brokenHash); string(v) != "bbbbbbbbbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbbbb")
	}
	c.colSet([]byte("bbb"), []byte("cccccc"), brokenHash)
	if v := c.colGet(nil, []byte("bbb"), brokenHash); string(v) != "cccccc" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "cccccc")
	}
	c.colSet([]byte("ccc"), []byte("dddddd"), brokenHash2)
	if v := c.colGet(nil, []byte("ccc"), brokenHash2); string(v) != "dddddd" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "dddddd")
	}
	c.colSet([]byte("ddd"), []byte("eeeeee"), brokenHash2)
	if v := c.colGet(nil, []byte("ddd"), brokenHash2); string(v) != "eeeeee" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "eeeeee")
	}
	c.Set([]byte("eee"), []byte("ffffff"))
	if v := c.Get(nil, []byte("eee")); string(v) != "ffffff" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ffffff")
	}
	if s := c.EntriesCount(); s != uint64(5) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 5)
	}
	if s := c.Size(); s != uint64(49) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 49)
	}
	if col := c.Collision(); col != 16 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 16)
	}

	// Delete all values
	c.colDel([]byte("bbb"), brokenHash)
	if v := c.colGet(nil, []byte("bbb"), brokenHash); string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if v := c.colGet(nil, []byte("aaa"), brokenHash); string(v) != "bbbbbbbbbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbbbbbbbbb")
	}
	c.colDel([]byte("aaa"), brokenHash)
	if v := c.colGet(nil, []byte("aaa"), brokenHash); string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	c.colDel([]byte("ccc"), brokenHash2)
	if v := c.colGet(nil, []byte("ccc"), brokenHash2); string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if v := c.colGet(nil, []byte("ddd"), brokenHash2); string(v) != "eeeeee" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "eeeeee")
	}
	c.colDel([]byte("ddd"), brokenHash2)
	if v := c.colGet(nil, []byte("ddd"), brokenHash2); string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if v := c.Get(nil, []byte("eee")); string(v) != "ffffff" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ffffff")
	}
	c.Del([]byte("eee"))
	if v := c.Get(nil, []byte("eee")); string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if s := c.EntriesCount(); s != uint64(0) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 0)
	}
	if s := c.Size(); s != uint64(0) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 0)
	}
	if col := c.Collision(); col != 20 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 20)
	}

	// Add first key with constant hash 1
	c.colSet([]byte("aaa"), []byte("bbb"), brokenHash)
	if v := c.colGet(nil, []byte("aaa"), brokenHash); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if s := c.EntriesCount(); s != uint64(1) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 1)
	}
	if s := c.Size(); s != uint64(6) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}
	if col := c.Collision(); col != 20 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 20)
	}
	// Add second key with constant hash 1
	c.colSet([]byte("bbb"), []byte("ccc"), brokenHash)
	if v := c.colGet(nil, []byte("bbb"), brokenHash); string(v) != "ccc" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ccc")
	}
	if s := c.EntriesCount(); s != uint64(2) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 2)
	}
	if s := c.Size(); s != uint64(12) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 12)
	}
	if col := c.Collision(); col != 22 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 22)
	}

	// Add first key with constant hash 2
	c.colSet([]byte("ccc"), []byte("ddd"), brokenHash2)
	if v := c.colGet(nil, []byte("ccc"), brokenHash2); string(v) != "ddd" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ddd")
	}
	if s := c.EntriesCount(); s != uint64(3) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 3)
	}
	if s := c.Size(); s != uint64(18) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 18)
	}
	if col := c.Collision(); col != 22 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 22)
	}

	// Add second key with constant hash 2
	c.colSet([]byte("ddd"), []byte("eee"), brokenHash2)
	if v := c.colGet(nil, []byte("ddd"), brokenHash2); string(v) != "eee" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "eee")
	}
	if s := c.EntriesCount(); s != uint64(4) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 4)
	}
	if s := c.Size(); s != uint64(24) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 24)
	}
	if col := c.Collision(); col != 24 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 24)
	}

	// Add 'normal' key
	c.Set([]byte("eee"), []byte("fff"))
	if v := c.Get(nil, []byte("eee")); string(v) != "fff" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "fff")
	}
	if s := c.EntriesCount(); s != uint64(5) {
		t.Fatalf("unexpected entries count obtained; got %d; want %d", s, 5)
	}
	if s := c.Size(); s != uint64(30) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 30)
	}
	if col := c.Collision(); col != 24 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 24)
	}

	// Check if all keys are there
	if v := c.colGet(nil, []byte("aaa"), brokenHash); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v := c.colGet(nil, []byte("bbb"), brokenHash); string(v) != "ccc" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ccc")
	}
	if v := c.colGet(nil, []byte("ccc"), brokenHash2); string(v) != "ddd" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ddd")
	}
	if v := c.colGet(nil, []byte("ddd"), brokenHash2); string(v) != "eee" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "eee")
	}
	if col := c.Collision(); col != 28 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 28)
	}

	// Check has with collision
	if v := c.colHas([]byte("aaa"), brokenHash); v != true {
		t.Fatalf("unexpected value obtained; got %v; want true", v)
	}
	if v := c.colHas([]byte("aaaaaa"), brokenHash); v != false {
		t.Fatalf("unexpected value obtained; got %v; want false", v)
	}
	if v := c.Has([]byte("eee")); v != true {
		t.Fatalf("unexpected value obtained; got %v; want true", v)
	}
	if v := c.Has([]byte("eeeeee")); v != false {
		t.Fatalf("unexpected value obtained; got %v; want false", v)
	}
	if col := c.Collision(); col != 30 {
		t.Fatalf("unexpected collision obtained; got %d; want %d", col, 30)
	}

	c.colDel([]byte("bbbbbb"), brokenHash)
	if v := c.colGet(nil, []byte("bbbbbb"), brokenHash2); string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	c.colDel([]byte("bbb"), brokenHash)
	if v := c.colGet(nil, []byte("bbb"), brokenHash); string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if v := c.colHas([]byte("bbb"), brokenHash); v != false {
		t.Fatalf("unexpected value obtained; got %v; want false", v)
	}
	c.Set([]byte("ffffffffffffffffffff"), []byte("gggggggggg"))
	if v := c.Get(nil, []byte("ffffffffffffffffffff")); string(v) != "gggggggggg" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "gggggggggg")
	}

}

func TestStorageStats(t *testing.T) {
	c := New()
	defer c.Reset()

	calls := uint64(5e6)

	for i := uint64(0); i < calls; i++ {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
		vv := c.Get(nil, k)
		if string(vv) != string(v) {
			t.Fatalf("unexpected value for key %q; got %q; want %q", k, vv, v)
		}
	}
	for i := uint64(0); i < calls/10; i++ {
		x := i * 10
		k := []byte(fmt.Sprintf("key %d", x))
		v := []byte(fmt.Sprintf("value %d", x))
		vv := c.Get(nil, k)
		if len(vv) > 0 && string(v) != string(vv) {
			t.Fatalf("unexpected value for key %q; got %q; want %q", k, vv, v)
		}
	}

	var s Stats
	c.UpdateStats(&s)
	getCalls := calls + calls/10
	if s.GetCalls != getCalls {
		t.Fatalf("unexpected number of getCalls; got %d; want %d", s.GetCalls, getCalls)
	}
	if s.SetCalls != calls {
		t.Fatalf("unexpected number of setCalls; got %d; want %d", s.SetCalls, calls)
	}
	if s.Misses != 0 {
		t.Fatalf("unexpected number of misses; got %d; it should be 0", s.Misses)
	}
	if s.Collisions != 0 {
		t.Fatalf("unexpected number of collisions; got %d; want 0", s.Collisions)
	}
	if s.EntriesCount != calls {
		t.Fatalf("unexpected number of items; got %d; cannot be smaller than %d", s.EntriesCount, calls)
	}
	if s.BytesSize < 1024 {
		t.Fatalf("unexpected BytesSize; got %d; cannot be smaller than %d", s.BytesSize, 1024)
	}
	s.Reset()
	// if s.MaxBytesSize < 32*1024*1024 {
	// 	t.Fatalf("unexpected MaxBytesSize; got %d; cannot be smaller than %d", s.MaxBytesSize, 32*1024*1024)
	// }
}

// func TestXXH3Collision(t *testing.T) {

// 	hashes := make(map[uint64]struct{})
// 	max := 100000000
// 	var b [512]byte
// 	for i := 0; i < max; i++ {

// 		if i%10000000 == 0 {
// 			t.Logf("iteration: %d\n", i)
// 		}

// 		_, err := rand.Read(b[:])
// 		if err != nil {
// 			t.Error(err)
// 			t.Fail()
// 		}

// 		result := xxh3.Hash(b[:])

// 		// Sanity check
// 		if i < 5 {
// 			fmt.Println(i, result)
// 		}

// 		if _, ok := hashes[result]; ok {
// 			t.Logf("%d == %v", result, b)
// 			t.Errorf("Found collision after %d searches", i)
// 		}

// 		hashes[result] = struct{}{}
// 	}

// 	fmt.Println("no collisions found", max)
// }

func TestStorageSmall(t *testing.T) {
	c := New()
	defer c.Reset()

	if v := c.Get(nil, []byte("aaa")); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}

	c.Set([]byte("key"), []byte("value"))
	if v := c.Get(nil, []byte("key")); string(v) != "value" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "value")
	}
	if v := c.Get(nil, nil); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}
	if v, exist := c.HasGet(nil, nil); exist {
		t.Fatalf("unexpected nil-keyed value obtained in small cache: %q", v)
	}
	if v := c.Get(nil, []byte("aaa")); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}

	c.Set([]byte("aaa"), []byte("bbb"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}

	c.Reset()
	if v := c.Get(nil, []byte("aaa")); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from empty cache: %q", v)
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}

	// Test empty value
	k := []byte("empty")
	c.Set(k, nil)
	if v := c.Get(nil, k); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from empty entry: %q", v)
	}
	if v, exist := c.HasGet(nil, k); !exist {
		t.Fatalf("cannot find empty entry for key %q", k)
	} else if len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from empty entry: %q", v)
	}
	if !c.Has(k) {
		t.Fatalf("cannot find empty entry for key %q", k)
	}
	if c.Has([]byte("foobar")) {
		t.Fatalf("non-existing entry found in the cache")
	}
}

func TestStorageMix(t *testing.T) {
	c := New()
	defer c.Reset()

	if v := c.Get(nil, []byte("aaa")); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}

	// First set
	c.Set([]byte("aaa"), []byte("bbb"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(6) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}

	// Long replace
	c.Set([]byte("aaa"), []byte("bbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefff"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "bbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefff" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefff")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefff" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefff")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(93) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 93)
	}

	// Short replace
	c.Set([]byte("aaa"), []byte("bbb"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(6) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}

	// Again Long replace
	c.Set([]byte("aaa"), []byte("bbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefff"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "bbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefff" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefff")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefff" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefffbbbcccdddeeefff")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(93) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 93)
	}

	// Again short replace
	c.Set([]byte("aaa"), []byte("bbb"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(6) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}

	// Fake Delete
	c.Del([]byte("bbb"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(6) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}

	// Not fake Delete
	c.Del([]byte("aaa"))
	if v := c.Get(nil, []byte("aaa")); v != nil {
		t.Fatalf("unexpected non-empty value got for key %q: %q", "aaa", v)
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if e := c.EntriesCount(); e != uint64(0) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 0)
	}
	if s := c.Size(); s != uint64(0) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 0)
	}

	// Set after delete with bigger len
	c.Set([]byte("aaaa"), []byte("bbbb"))
	if v := c.Get(nil, []byte("aaaa")); string(v) != "bbbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaaa")); !exist || string(v) != "bbbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbbb")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(8) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}

	// Reset storage
	c.Reset()
	if v := c.Get(nil, []byte("aaa")); string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if e := c.EntriesCount(); e != uint64(0) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 0)
	}
	if s := c.Size(); s != uint64(0) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 0)
	}

	// Set after reset
	c.Set([]byte("aaa"), []byte("bbb"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(6) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}

	c.Del([]byte("aaa"))
	if v := c.Get(nil, []byte("aaa")); v != nil {
		t.Fatalf("unexpected non-empty value got for key %q: %q", "aaa", v)
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if e := c.EntriesCount(); e != uint64(0) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 0)
	}
	if s := c.Size(); s != uint64(0) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 0)
	}

	// Set after delete with bigger len
	c.Set([]byte("aaaa"), []byte("bbbb"))
	if v := c.Get(nil, []byte("aaaa")); string(v) != "bbbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaaa")); !exist || string(v) != "bbbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbbb")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(8) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}

	// Set bigger key/val then deleted
	// Reset storage
	c.Reset()
	if v := c.Get(nil, []byte("aaa")); string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if e := c.EntriesCount(); e != uint64(0) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 0)
	}
	if s := c.Size(); s != uint64(0) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 0)
	}

	// Set after reset
	c.Set([]byte("aaa"), []byte("bbb"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(6) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}

	// Delete after reset
	c.Del([]byte("aaa"))
	if v := c.Get(nil, []byte("aaa")); v != nil {
		t.Fatalf("unexpected non-empty value got for key %q: %q", "aaa", v)
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if e := c.EntriesCount(); e != uint64(0) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 0)
	}
	if s := c.Size(); s != uint64(0) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 0)
	}

	// Set after reset
	c.Set([]byte("aaaaaaaaaa"), []byte("bbbbbbbbbb"))
	if v := c.Get(nil, []byte("aaaaaaaaaa")); string(v) != "bbbbbbbbbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbbbbbbbbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaaaaaaaaa")); !exist || string(v) != "bbbbbbbbbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbbbbbbbbb")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(20) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 20)
	}

	c.Reset()
	if v := c.Get(nil, []byte("aaa")); string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if e := c.EntriesCount(); e != uint64(0) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 0)
	}
	if s := c.Size(); s != uint64(0) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 0)
	}

	// Replace value with smaller length
	c.Set([]byte("aaa"), []byte("bbb"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(6) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}

	c.Set([]byte("aaa"), []byte("b"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "b" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "b")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "b" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "b")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(4) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 4)
	}

	c.Reset()
	if v := c.Get(nil, []byte("aaa")); string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if e := c.EntriesCount(); e != uint64(0) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 0)
	}
	if s := c.Size(); s != uint64(0) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 0)
	}

	// Another set
	c.Set([]byte("aaa"), []byte("bbb"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(6) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}

	c.Set([]byte("ccc"), []byte("ddd"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v := c.Get(nil, []byte("ccc")); string(v) != "ddd" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ddd")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.HasGet(nil, []byte("ccc")); !exist || string(v) != "ddd" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "ddd")
	}
	if e := c.EntriesCount(); e != uint64(2) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 2)
	}
	if s := c.Size(); s != uint64(12) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 12)
	}

	// Reset storage
	c.Reset()
	if v := c.Get(nil, []byte("aaa")); string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if e := c.EntriesCount(); e != uint64(0) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 0)
	}
	if s := c.Size(); s != uint64(0) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 0)
	}

	// Set nil key - nil value
	c.Set(nil, nil)
	if v := c.Get(nil, nil); string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if v, exist := c.HasGet(nil, nil); !exist || string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(0) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 0)
	}

	// Replace value for nil key
	c.Set(nil, []byte("nil"))
	if v := c.Get(nil, nil); string(v) != "nil" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "nil")
	}
	if v, exist := c.HasGet(nil, nil); !exist || string(v) != "nil" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "nil")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(3) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 3)
	}

	// Delete nil key
	c.Del(nil)
	if v := c.Get(nil, nil); v != nil {
		t.Fatalf("unexpected non-empty value got for key %v: %q", nil, v)
	}
	if v, exist := c.HasGet(nil, nil); exist || string(v) != "" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "")
	}
	if e := c.EntriesCount(); e != uint64(0) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 0)
	}
	if s := c.Size(); s != uint64(0) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 0)
	}

	// Key change after set
	testKey := []byte("aaa")
	testVal := []byte("bbb")
	c.Set(testKey, testVal)
	testKey[0]++
	testVal[0]++

	if v := c.Get(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(6) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}
	if string(testKey) != "baa" || string(testVal) != "cbb" {
		t.Fatalf("unexpected slice; got %q, %q; want %q, %q", testKey, testVal, "baa", "cbb")
	}

	// Key change after get
	nv := c.Get(nil, []byte("aaa"))
	if string(nv) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", nv, "bbb")
	}
	nv[0]++

	if v := c.Get(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if e := c.EntriesCount(); e != uint64(1) {
		t.Fatalf("unexpected entires count obtained; got %d; want %d", e, 1)
	}
	if s := c.Size(); s != uint64(6) {
		t.Fatalf("unexpected size obtained; got %d; want %d", s, 6)
	}
	if string(testKey) != "baa" || string(testVal) != "cbb" {
		t.Fatalf("unexpected slice; got %q, %q; want %q, %q", testKey, testVal, "baa", "cbb")
	}

}

func TestStorageBigKeyValue(t *testing.T) {
	c := New()
	defer c.Reset()

	// Both key and value exceed 64Kb
	k := make([]byte, 90*1024)
	v := make([]byte, 100*1024)
	c.Set(k, v)
	vv := c.Get(nil, k)
	if len(vv) != 100*1024 {
		t.Fatalf("unexpected value got for key %q; got %d; want %d", k, vv, 100*1024)
	}

	// len(key) + len(value) > 64Kb
	k = make([]byte, 40*1024)
	v = make([]byte, 40*1024)
	c.Set(k, v)
	vv = c.Get(nil, k)
	if len(vv) != 40*1024 {
		t.Fatalf("unexpected value got for key %q; got %d; want %d", k, vv, 40*1024)
	}
}

func TestStorageDel(t *testing.T) {
	c := New()
	defer c.Reset()
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
		vv := c.Get(nil, k)
		if string(vv) != string(v) {
			t.Fatalf("unexpected value for key %q; got %q; want %q", k, vv, v)
		}
		c.Del(k)
		vv = c.Get(nil, k)
		if len(vv) > 0 {
			t.Fatalf("unexpected non-empty value got for key %q: %q", k, vv)
		}
	}
}

func TestStorageReplace(t *testing.T) {
	c := New()
	defer c.Reset()
	for i := 0; i < 100; i++ {
		k := []byte("key")
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
		vv := c.Get(nil, k)
		if string(vv) != string(v) {
			t.Fatalf("unexpected value for key %q; got %q; want %q", k, vv, v)
		}
	}
}

func TestStorageSetGetSerial(t *testing.T) {
	itemsCount := 10000
	c := New()
	defer c.Reset()
	if err := testStorageGetSet(c, itemsCount); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestStorageGetSetConcurrent(t *testing.T) {
	itemsCount := 10000
	const gorotines = 10
	c := New()
	defer c.Reset()

	ch := make(chan error, gorotines)
	for i := 0; i < gorotines; i++ {
		go func() {
			ch <- testStorageGetSet(c, itemsCount)
		}()
	}
	for i := 0; i < gorotines; i++ {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout")
		}
	}
	if c.Collision() != 0 {
		t.Fatalf("Find collision!")
	}
}

// Writing 10k keys with same hash (crazy)
func TestStorageGetSetCollisionConcurrent(t *testing.T) {
	itemsCount := 1000
	const gorotines = 10
	c := New()
	defer c.Reset()

	ch := make(chan error, gorotines)
	for i := 0; i < gorotines; i++ {
		go func() {
			ch <- testStorageColGetSet(c, itemsCount)
		}()
	}
	for i := 0; i < gorotines; i++ {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

		case <-time.After(5 * time.Second):
			t.Fatalf("timeout")
		}
	}
}

func TestStorageSetDeleteConcurrent(t *testing.T) {
	itemsCount := 10000
	const gorotines = 10
	c := New()
	defer c.Reset()

	ch := make(chan error, gorotines)
	for i := 0; i < gorotines; i++ {
		go func() {
			ch <- testStorageSetDelete(c, itemsCount)
		}()
	}
	for i := 0; i < gorotines; i++ {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout")
		}
	}
}

func testStorageGetSet(c *Storage, itemsCount int) error {
	for i := 0; i < itemsCount; i++ {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
		vv := c.Get(nil, k)
		if string(vv) != string(v) {
			return fmt.Errorf("unexpected value for key %q after insertion; got %q; want %q", k, vv, v)
		}
	}
	for i := 0; i < itemsCount; i++ {
		k := []byte(fmt.Sprintf("key %d", i))
		vExpected := fmt.Sprintf("value %d", i)
		v := c.Get(nil, k)
		if string(v) != string(vExpected) {
			if len(v) > 0 {
				return fmt.Errorf("unexpected value for key %q after all insertions; got %q; want %q", k, v, vExpected)
			}
		}
	}
	return nil
}

func testStorageColGetSet(c *Storage, itemsCount int) error {
	for i := 0; i < itemsCount; i++ {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.colSet(k, v, brokenHash)
		vv := c.colGet(nil, k, brokenHash)
		if string(vv) != string(v) {
			return fmt.Errorf("unexpected value for key %q after insertion; got %q; want %q", k, vv, v)
		}
	}
	for i := 0; i < itemsCount; i++ {
		k := []byte(fmt.Sprintf("key %d", i))
		vExpected := fmt.Sprintf("value %d", i)
		v := c.colGet(nil, k, brokenHash)
		if string(v) != string(vExpected) {
			if len(v) > 0 {
				return fmt.Errorf("unexpected value for key %q after all insertions; got %q; want %q", k, v, vExpected)
			}
		}
	}
	return nil
}

func testStorageSetDelete(c *Storage, itemsCount int) error {
	for i := 0; i < itemsCount; i++ {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
		c.Del(k)
	}
	for i := 0; i < itemsCount; i++ {
		k := []byte(fmt.Sprintf("key %d", i))
		vExpected := fmt.Sprintf("value %d", i)
		v := c.Get(nil, k)
		if string(v) != string(vExpected) {
			if len(v) > 0 {
				return fmt.Errorf("unexpected value for key %q after all insertions; got %q; want %q", k, v, vExpected)
			}
		}
	}
	return nil
}

func TestStorageResetUpdateStatsSetConcurrent(t *testing.T) {
	c := New()

	stopCh := make(chan struct{})

	// run workers for cache reset
	var resettersWG sync.WaitGroup
	for i := 0; i < 10; i++ {
		resettersWG.Add(1)
		go func() {
			defer resettersWG.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					c.Reset()
					runtime.Gosched()
				}
			}
		}()
	}

	// run workers for update cache stats
	var statsWG sync.WaitGroup
	for i := 0; i < 10; i++ {
		statsWG.Add(1)
		go func() {
			defer statsWG.Done()
			var s Stats
			for {
				select {
				case <-stopCh:
					return
				default:
					c.UpdateStats(&s)
					runtime.Gosched()
				}
			}
		}()
	}

	// run workers for setting data to cache
	var settersWG sync.WaitGroup
	for i := 0; i < 10; i++ {
		settersWG.Add(1)
		go func() {
			defer settersWG.Done()
			for j := 0; j < 100; j++ {
				key := []byte(fmt.Sprintf("key_%d", j))
				value := []byte(fmt.Sprintf("value_%d", j))
				c.Set(key, value)
				runtime.Gosched()
			}
		}()
	}

	// wait for setters
	settersWG.Wait()
	close(stopCh)
	statsWG.Wait()
	resettersWG.Wait()
}
