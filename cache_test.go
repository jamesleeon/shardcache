package cache

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// --- 1. Unit Tests (Functionality Verification) ---

// TestCacheSetGet verifies basic Set and Get operations.
func TestCacheSetGet(t *testing.T) {
	c := New(0)
	c.Set("key1", "value1", time.Minute)
	val, found := c.Get("key1")
	if !found || val.(string) != "value1" {
		t.Fatal("Failed to get value that was set")
	}

	_, found = c.Get("key-not-exist")
	if found {
		t.Fatal("Got value for key that was not set")
	}
}

// TestCacheExpiry verifies TTL and expiration logic.
func TestCacheExpiry(t *testing.T) {
	c := New(10 * time.Millisecond) // Fast cleanup interval
	c.Set("key1", "value1", 50*time.Millisecond)
	c.Set("key2", "value2", 0) // No expiration (TTL=0)

	// Immediate access
	val, found := c.Get("key1")
	if !found || val.(string) != "value1" {
		t.Fatal("Failed to get value immediately after set")
	}

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// key1 should have expired
	_, found = c.Get("key1")
	if found {
		t.Fatal("Got value for key that should have expired")
	}

	// key2 should still exist
	_, found = c.Get("key2")
	if !found {
		t.Fatal("Got value for key that should not expire")
	}

	// Test Exists method
	if c.Exists("key1") {
		t.Fatal("Exists() returned true for expired key")
	}
	if !c.Exists("key2") {
		t.Fatal("Exists() returned false for non-expired key")
	}
}

// TestCacheSetVariations verifies SetNX, SetIfNotExists, and Replace logic.
func TestCacheSetVariations(t *testing.T) {
	c := New(0)
	c.Set("key1", "value1", time.Minute)

	// Test SetIfNotExists on existing key
	set := c.SetIfNotExists("key1", "new-value", time.Minute)
	if set {
		t.Fatal("SetIfNotExists should have failed for existing key")
	}
	val, _ := c.Get("key1")
	if val.(string) != "value1" {
		t.Fatal("SetIfNotExists incorrectly overwrote value")
	}
	// Test SetIfNotExists on new key
	set = c.SetIfNotExists("key2", "value2", time.Minute)
	if !set {
		t.Fatal("SetIfNotExists should have succeeded for new key")
	}

	// Test SetNX (alias for SetIfNotExists)
	set = c.SetNX("key2", "new-value", time.Minute)
	if set {
		t.Fatal("SetNX should have failed for existing key")
	}

	// Test Replace on non-existent key
	set = c.Replace("key-not-exist", "value", time.Minute)
	if set {
		t.Fatal("Replace should have failed for non-existent key")
	}
	// Test Replace on existing key
	set = c.Replace("key1", "value-replaced", time.Minute)
	if !set {
		t.Fatal("Replace should have succeeded for existing key")
	}
	val, _ = c.Get("key1")
	if val.(string) != "value-replaced" {
		t.Fatal("Replace did not set the correct value")
	}
}

// TestCacheDeleteAndClear verifies Del and Clear operations.
func TestCacheDeleteAndClear(t *testing.T) {
	c := New(0)
	c.Set("key1", "value1", time.Minute)
	c.Set("key2", "value2", time.Minute)
	c.Set("key3", "value3", time.Minute)

	// Test Del
	c.Del("key1")
	_, found := c.Get("key1")
	if found {
		t.Fatal("Failed to delete key")
	}

	// Test Count
	if c.Count() != 2 {
		t.Fatalf("Expected count 2, got %d", c.Count())
	}

	// Test Clear
	c.Clear()
	if c.Count() != 0 {
		t.Fatalf("Expected count 0 after Clear, got %d", c.Count())
	}
	_, found = c.Get("key2")
	if found {
		t.Fatal("Got value after Clear")
	}
}

// TestCacheCleanup verifies the background cleanup goroutine.
func TestCacheCleanup(t *testing.T) {
	c := New(20 * time.Millisecond)
	c.Set("key1", "value", 10*time.Millisecond) // Should expire immediately
	c.Set("key2", "value", 10*time.Millisecond) // Should expire immediately

	// Wait for cleanup goroutine to run
	time.Sleep(50 * time.Millisecond)

	// Keys should be deleted by the background routine
	if c.Count() != 0 {
		t.Fatalf("Cleanup did not remove expired items, count: %d", c.Count())
	}
	if c.EvictedTotal() != 2 {
		t.Fatalf("EvictedTotal expected 2, got %d", c.EvictedTotal())
	}
}

// TestCacheCompareAndDelete verifies CompareAndDelete operation.
func TestCacheCompareAndDelete(t *testing.T) {
	c := New(0)
	c.Set("key1", "value1", time.Minute)

	// Fail: incorrect value
	if c.CompareAndDelete("key1", "wrong_value") {
		t.Fatal("CompareAndDelete succeeded with incorrect value")
	}
	if _, found := c.Get("key1"); !found {
		t.Fatal("CompareAndDelete incorrectly deleted key")
	}

	// Success: correct value
	if !c.CompareAndDelete("key1", "value1") {
		t.Fatal("CompareAndDelete failed with correct value")
	}
	if _, found := c.Get("key1"); found {
		t.Fatal("CompareAndDelete failed to delete key on success")
	}

	// Fail: key doesn't exist
	if c.CompareAndDelete("key-nonexistent", "value") {
		t.Fatal("CompareAndDelete succeeded on non-existent key")
	}
}

// --- 2. Persistence Tests (Save/Load) ---

// stringSerializer is a simple GOB serializer for test purposes.
type stringSerializer struct{}

func (s *stringSerializer) Serialize(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *stringSerializer) Deserialize(data []byte) (interface{}, error) {
	var value string // Assuming we only store strings for this test
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

// TestCacheSaveLoad verifies Save and Load operations, including skipping expired items during Load.
func TestCacheSaveLoad(t *testing.T) {
	c := New(0)
	serializer := &stringSerializer{}
	testFile := "test_cache.dat"
	defer os.Remove(testFile)

	c.Set("key1", "value1", time.Minute)
	c.Set("key2", "value2", 0)                  // No expiration
	c.Set("key3", "value3", 1*time.Millisecond) // Should be expired when saved/loaded

	time.Sleep(2 * time.Millisecond) // Ensure key3 expires

	if err := c.Save(testFile, serializer); err != nil {
		t.Fatalf("Failed to save cache: %v", err)
	}

	// Create new cache instance and load
	c2 := New(0)
	if err := c2.Load(testFile, serializer); err != nil {
		t.Fatalf("Failed to load cache: %v", err)
	}

	// key1 and key2 should exist
	val1, found1 := c2.Get("key1")
	if !found1 || val1.(string) != "value1" {
		t.Fatal("Failed to load key1")
	}
	val2, found2 := c2.Get("key2")
	if !found2 || val2.(string) != "value2" {
		t.Fatal("Failed to load key2")
	}

	// key3 should have been skipped during Save/Load due to expiration
	_, found3 := c2.Get("key3")
	if found3 {
		t.Fatal("Loaded expired key3")
	}

	if c2.Count() != 2 {
		t.Fatalf("Expected count 2 after load, got %d", c2.Count())
	}
}

// --- 3. Benchmark Tests (Performance Measurement) ---

const (
	benchmarkItemCount = 10000 // Number of keys to pre-populate for mixed tests
)

// fillCacheWithStrings is a helper function to pre-populate the cache for benchmarks.
func fillCacheWithStrings(c *Cache, count int) []string {
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		k := fmt.Sprintf("key-%d", i)
		keys[i] = k
		c.Set(k, "value", time.Hour)
	}
	return keys
}

// BenchmarkCacheGetHit - Measures concurrent reads for a single hot key.
func BenchmarkCacheGetHit(b *testing.B) {
	c := New(0)
	c.Set("key", "value", time.Hour)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Get("key")
		}
	})
}

// BenchmarkCacheGetMiss - Measures concurrent reads for non-existent keys.
func BenchmarkCacheGetMiss(b *testing.B) {
	c := New(0)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Reading a static missing key to test consistent miss latency
			c.Get("key-miss")
		}
	})
}

// BenchmarkCacheSetNew - Measures concurrent writes for unique new keys.
func BenchmarkCacheSetNew(b *testing.B) {
	c := New(0)
	var counter uint64 // Atomic counter for unique keys
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Write a different key for each operation
			k := atomic.AddUint64(&counter, 1)
			c.Set(strconv.FormatUint(k, 10), "value", time.Hour)
		}
	})
}

// BenchmarkCacheSetIfNotExistsNew - Measures concurrent SetIfNotExists for unique new keys.
func BenchmarkCacheSetIfNotExistsNew(b *testing.B) {
	c := New(0)
	var counter uint64 // Atomic counter for unique keys
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Write a different key for each operation
			k := atomic.AddUint64(&counter, 1)
			c.SetIfNotExists(strconv.FormatUint(k, 10), "value", time.Hour)
		}
	})
}

// BenchmarkCacheSetOverwrite - Measures concurrent writes overwriting existing keys.
// This highlights the efficiency of the sync.Pool item recycling (0 allocs/op).
func BenchmarkCacheSetOverwrite(b *testing.B) {
	c := New(0)
	// Pre-populate enough keys (100k) to ensure good sharding
	keys := fillCacheWithStrings(c, 100000)
	keyCount := len(keys)

	// Create a single random source for the whole benchmark run
	r := rand.New(rand.NewSource(0))

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// Use a simple index based on the goroutine ID and iteration count for key selection
		// This avoids contention on a single shared RNG source
		p := r.Intn(keyCount) // Start index offset
		i := 0

		for pb.Next() {
			k := keys[(p + i) % keyCount]
			c.Set(k, "new-value", time.Hour)
			i++
		}
	})
}

// BenchmarkCacheGetSetMixed - Measures concurrent mixed read/write (90% read, 10% write).
// This simulates the most realistic production load, leveraging sharding heavily.
func BenchmarkCacheGetSetMixed(b *testing.B) {
	c := New(0)
	keys := fillCacheWithStrings(c, benchmarkItemCount)
	keyCount := len(keys)

	r := rand.New(rand.NewSource(0))

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// Use a locally seeded RNG for fast, non-contended random decisions
		// We use a different seed for each goroutine to ensure diversity
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))

		for pb.Next() {
			k := keys[rng.Intn(keyCount)]

			// 90% Read
			if rng.Intn(10) != 0 {
				c.Get(k)
			} else {
				// 10% Write
				c.Set(k, "new-value-mixed", time.Hour)
			}
		}
	})
}