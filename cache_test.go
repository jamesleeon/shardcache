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

// --- 1. 单元测试 (确保功能正确) ---

// TestCacheSetGet 测试基本的 Set 和 Get
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

// TestCacheExpiry 测试 TTL 和过期
func TestCacheExpiry(t *testing.T) {
	c := New(10 * time.Millisecond) // 快速清理
	c.Set("key1", "value1", 50*time.Millisecond)
	c.Set("key2", "value2", 0) // 永不过期

	// 立即获取
	val, found := c.Get("key1")
	if !found || val.(string) != "value1" {
		t.Fatal("Failed to get value immediately after set")
	}

	// 等待过期
	time.Sleep(100 * time.Millisecond)

	// 此时 key1 应该已过期
	_, found = c.Get("key1")
	if found {
		t.Fatal("Got value for key that should have expired")
	}

	// key2 应该仍然存在
	_, found = c.Get("key2")
	if !found {
		t.Fatal("Got value for key that should not expire")
	}

	// 测试 Exists 方法
	if c.Exists("key1") {
		t.Fatal("Exists() returned true for expired key")
	}
	if !c.Exists("key2") {
		t.Fatal("Exists() returned false for non-expired key")
	}
}

// TestCacheSetVariations 测试 SetNX, SetIfNotExists, Replace
func TestCacheSetVariations(t *testing.T) {
	c := New(0)
	c.Set("key1", "value1", time.Minute)

	// SetIfNotExists
	set := c.SetIfNotExists("key1", "new-value", time.Minute)
	if set {
		t.Fatal("SetIfNotExists should have failed for existing key")
	}
	val, _ := c.Get("key1")
	if val.(string) != "value1" {
		t.Fatal("SetIfNotExists incorrectly overwrote value")
	}
	set = c.SetIfNotExists("key2", "value2", time.Minute)
	if !set {
		t.Fatal("SetIfNotExists should have succeeded for new key")
	}

	// SetNX (等同于 SetIfNotExists)
	set = c.SetNX("key2", "new-value", time.Minute)
	if set {
		t.Fatal("SetNX should have failed for existing key")
	}

	// Replace
	set = c.Replace("key-not-exist", "value", time.Minute)
	if set {
		t.Fatal("Replace should have failed for non-existent key")
	}
	set = c.Replace("key1", "value-replaced", time.Minute)
	if !set {
		t.Fatal("Replace should have succeeded for existing key")
	}
	val, _ = c.Get("key1")
	if val.(string) != "value-replaced" {
		t.Fatal("Replace did not set the correct value")
	}
}

// TestCacheDeleteAndClear 测试 Del 和 Clear
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

// TestCacheCleanup 测试后台清理
func TestCacheCleanup(t *testing.T) {
	c := New(20 * time.Millisecond)
	c.Set("key1", "value", 10*time.Millisecond) // 立即过期
	c.Set("key2", "value", 10*time.Millisecond) // 立即过期

	// 等待 cleanup goroutine 运行
	time.Sleep(50 * time.Millisecond)

	// 此时 key 应该被后台删除了
	if c.Count() != 0 {
		t.Fatalf("Cleanup did not remove expired items, count: %d", c.Count())
	}
	if c.EvictedTotal() != 2 {
		t.Fatalf("EvictedTotal expected 2, got %d", c.EvictedTotal())
	}
}

// --- 2. 持久化测试 (Save/Load) ---

// stringSerializer 是一个简单的序列化器，用于测试
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
	var value string // 假设我们只存字符串
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func TestCacheSaveLoad(t *testing.T) {
	c := New(0)
	serializer := &stringSerializer{}
	testFile := "test_cache.dat"
	defer os.Remove(testFile)

	c.Set("key1", "value1", time.Minute)
	c.Set("key2", "value2", 0)                  // 永不过期
	c.Set("key3", "value3", 1*time.Millisecond) // 加载时会过期

	time.Sleep(2 * time.Millisecond) // 确保 key3 过期

	if err := c.Save(testFile, serializer); err != nil {
		t.Fatalf("Failed to save cache: %v", err)
	}

	// 创建新缓存实例并加载
	c2 := New(0)
	if err := c2.Load(testFile, serializer); err != nil {
		t.Fatalf("Failed to load cache: %v", err)
	}

	// key1 和 key2 应该存在
	val1, found1 := c2.Get("key1")
	if !found1 || val1.(string) != "value1" {
		t.Fatal("Failed to load key1")
	}
	val2, found2 := c2.Get("key2")
	if !found2 || val2.(string) != "value2" {
		t.Fatal("Failed to load key2")
	}

	// key3 应该因为过期而未被保存/加载
	_, found3 := c2.Get("key3")
	if found3 {
		t.Fatal("Loaded expired key3")
	}

	if c2.Count() != 2 {
		t.Fatalf("Expected count 2 after load, got %d", c2.Count())
	}
}

// --- 3. 性能基准测试 ---

const (
	benchmarkItemCount = 10000 // 预填充1万个key用于测试
)

// fillCacheWithStrings 是一个辅助函数，用于为基准测试预填充缓存
// 返回一个 key 列表供后续随机访问
func fillCacheWithStrings(c *Cache, count int) []string {
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		k := fmt.Sprintf("key-%d", i)
		keys[i] = k
		c.Set(k, "value", time.Hour)
	}
	return keys
}

// BenchmarkCacheGetHit (你原有的测试) - 测试并发读 (热点 Key)
// 这测试的是 RLock 的性能
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

// BenchmarkCacheGetMiss - 测试并发读 (Key 不存在)
func BenchmarkCacheGetMiss(b *testing.B) {
	c := New(0)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// 每个 goroutine 读取不同的不存在的 key，以测试分片
		// (这里我们用 rand，但在并发中用 atomic 更标准)
		// (不过对于 'miss' 来说，key 内容不重要)
		for pb.Next() {
			c.Get("key-miss")
		}
	})
}

// BenchmarkCacheSetNew - 测试并发写 (新 Key)
// 这测试的是分片锁（写锁）的性能
func BenchmarkCacheSetNew(b *testing.B) {
	c := New(0)
	var counter uint64 // 使用原子计数器确保 key 唯一
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 每个 goroutine 写入不同的 key
			k := atomic.AddUint64(&counter, 1)
			c.Set(strconv.FormatUint(k, 10), "value", time.Hour)
		}
	})
}

// BenchmarkCacheSetIfNotExistsNew - 测试并发写 (新 Key)
// 这测试的是分片锁（写锁）的性能
func BenchmarkCacheSetIfNotExistsNew(b *testing.B) {
	c := New(0)
	var counter uint64 // 使用原子计数器确保 key 唯一
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 每个 goroutine 写入不同的 key
			k := atomic.AddUint64(&counter, 1)
			c.SetIfNotExists(strconv.FormatUint(k, 10), "value", time.Hour)
		}
	})
}

// BenchmarkCacheSetOverwrite - 测试并发写 (覆盖旧 Key)
// 这测试的是分片锁（写锁）+ map 内部的开销
func BenchmarkCacheSetOverwrite(b *testing.B) {
	c := New(0)
	// 预填充数据，key 足够多 (100k) 以分散到所有分片
	keys := fillCacheWithStrings(c, 100000)
	keyCount := len(keys)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// 每个 goroutine 随机覆盖一个 key
		// 使用本地的 rand 减少全局锁争用
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			k := keys[rng.Intn(keyCount)]
			c.Set(k, "new-value", time.Hour)
		}
	})
}

// BenchmarkCacheGetSetMixed - 测试并发混合读写 (90% 读, 10% 写)
// 这是最真实的场景
func BenchmarkCacheGetSetMixed(b *testing.B) {
	c := New(0)
	keys := fillCacheWithStrings(c, benchmarkItemCount)
	keyCount := len(keys)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			k := keys[rng.Intn(keyCount)]

			// 90% 读
			if rng.Intn(10) != 0 {
				c.Get(k)
			} else {
				// 10% 写
				c.Set(k, "new-value-mixed", time.Hour)
			}
		}
	})
}
