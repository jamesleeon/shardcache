package cache

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Serializer defines the interface for custom value serialization.
type Serializer interface {
	// Serialize converts the value to a byte array.
	Serialize(value interface{}) ([]byte, error)
	// Deserialize reconstructs the value from a byte array.
	Deserialize(data []byte) (interface{}, error)
}

// item is for internal use only.
type item struct {
	value  interface{}
	expiry int64 // Uses Unix nanoseconds timestamp to avoid time.Time overhead.
}

// itemPool reuses item objects to reduce GC pressure.
var itemPool = sync.Pool{
	New: func() interface{} {
		return &item{}
	},
}

// acquireItem gets an item from the pool and initializes it.
func acquireItem(value interface{}, expiry int64) *item {
	i := itemPool.Get().(*item)
	i.value = value
	i.expiry = expiry
	return i
}

// releaseItem returns the item to the pool and cleans up fields.
func releaseItem(i *item) {
	// Clean up fields, especially the interface{} (value), to help GC.
	i.value = nil
	i.expiry = 0
	itemPool.Put(i)
}

// shard structure, designed for concurrency.
type shard struct {
	mu    sync.RWMutex
	items map[string]*item
	// Pad to align structure fields to avoid cache line contention (False Sharing).
	// 40 bytes is often enough to push the next field onto a new cache line.
	_ [40]byte
}

// Cache is a sharded, concurrent cache structure.
type Cache struct {
	shards       []*shard
	shardMask    uint32 // Uses bitmask instead of modulo for speed
	stop         chan struct{}
	evictedTotal atomic.Uint64
}

// New creates a new Cache instance with 256 default shards.
func New(cleanupInterval time.Duration) *Cache {
	return NewWithShardCount(cleanupInterval, 256)
}

// NewWithShardCount creates a Cache instance with the specified number of shards.
// shardCount must be a power of two; it will be adjusted if necessary.
func NewWithShardCount(cleanupInterval time.Duration, shardCount uint32) *Cache {
	// Ensure shard count is a power of two for faster bitwise modulo.
	if shardCount == 0 {
		shardCount = 256
	}
	shardCount = nextPowerOfTwo(shardCount)

	c := &Cache{
		shards:    make([]*shard, shardCount),
		shardMask: shardCount - 1, // Used for fast modulo operation
		stop:      make(chan struct{}),
	}

	// Initialize all shards
	for i := uint32(0); i < shardCount; i++ {
		c.shards[i] = &shard{
			items: make(map[string]*item),
		}
	}

	if cleanupInterval > 0 {
		go c.cleanup(cleanupInterval)
	}

	return c
}

// nextPowerOfTwo returns the smallest power of two greater than or equal to n.
func nextPowerOfTwo(n uint32) uint32 {
	if n == 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	return n + 1
}

// getShard returns the correct shard for a given key.
func (c *Cache) getShard(key string) *shard {
	hash := fnv32a(key)
	return c.shards[hash&c.shardMask] // Bitwise modulo is faster
}

// Count returns the number of items in the cache.
func (c *Cache) Count() int {
	count := 0
	for _, s := range c.shards {
		s.mu.RLock()
		count += len(s.items)
		s.mu.RUnlock()
	}
	return count
}

// Set unconditionally sets a cache entry (overwrites existing key).
func (c *Cache) Set(key string, value interface{}, ttl time.Duration) {
	var expiry int64
	if ttl > 0 {
		expiry = time.Now().Add(ttl).UnixNano()
	}

	s := c.getShard(key)
	s.mu.Lock()

	// If the key already exists, release the old item first.
	if oldItem, exists := s.items[key]; exists {
		releaseItem(oldItem)
	}

	// Acquire new item from the pool
	s.items[key] = acquireItem(value, expiry)
	s.mu.Unlock()
}

// Exists checks if a key exists and is not expired.
func (c *Cache) Exists(key string) bool {
	s := c.getShard(key)
	s.mu.RLock()
	item1, ok := s.items[key]
	s.mu.RUnlock()

	if !ok {
		return false
	}
	if item1.expiry > 0 && item1.expiry < time.Now().UnixNano() {
		return false
	}
	return true
}

// SetIfNotExists sets the key only if it does not already exist and is not expired.
func (c *Cache) SetIfNotExists(key string, value interface{}, ttl time.Duration) bool {
	s := c.getShard(key)
	nowNano := time.Now().UnixNano()

	// --- 1. Fast path: RLock check ---
	s.mu.RLock()
	obj, exists := s.items[key]
	if exists {
		if obj.expiry == 0 || obj.expiry > nowNano {
			s.mu.RUnlock()
			return false // Exists and not expired, fast return
		}
	}
	s.mu.RUnlock()

	// --- 2. Slow path: Requires write (does not exist or is expired) ---
	var expiry int64
	if ttl > 0 {
		expiry = nowNano + ttl.Nanoseconds()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// --- 3. Double Check: Another goroutine might have set it ---
	if objItem, ok := s.items[key]; ok {
		if objItem.expiry == 0 || objItem.expiry > nowNano {
			return false // Exists and not expired, exit
		}
		// It was expired, release the old item
		releaseItem(objItem)
	}

	// --- 4. Write ---
	s.items[key] = acquireItem(value, expiry)
	return true
}

// Replace replaces the value only if the key exists and is not expired.
func (c *Cache) Replace(key string, value interface{}, ttl time.Duration) bool {
	s := c.getShard(key)
	now := time.Now().UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if key exists and is not expired
	if itemObject, ok := s.items[key]; ok {
		if itemObject.expiry == 0 || itemObject.expiry > now {
			// Exists and not expired, release the old item
			releaseItem(itemObject)

			// Perform replacement
			var expiry int64
			if ttl > 0 {
				expiry = time.Now().Add(ttl).UnixNano()
			}

			s.items[key] = acquireItem(value, expiry)
			return true
		}
	}

	return false // Key does not exist or is expired
}

// CompareAndDelete deletes the cache entry only if its current value matches the expected value.
func (c *Cache) CompareAndDelete(key string, expectedValue interface{}) bool {
	s := c.getShard(key)
	now := time.Now().UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	obj, ok := s.items[key]
	if !ok {
		return false // Key does not exist
	}

	// Check expiry
	if obj.expiry > 0 && obj.expiry < now {
		delete(s.items, key)
		releaseItem(obj)
		return false // Expired
	}

	// Compare values
	if obj.value == expectedValue {
		delete(s.items, key)
		releaseItem(obj)
		return true // Successfully deleted
	}

	return false // Values do not match
}

// Get retrieves a cache entry. This is the **concurrently safe** version.
func (c *Cache) Get(key string) (interface{}, bool) {
	s := c.getShard(key)
	s.mu.RLock()
	obj, ok := s.items[key]

	if !ok {
		s.mu.RUnlock()
		return nil, false
	}

	// 1. Check expiry inside the lock to ensure atomicity of the item check.
	if obj.expiry > 0 && obj.expiry < time.Now().UnixNano() {
		s.mu.RUnlock()
		return nil, false
	}

	// 2. Read the value inside the lock, preventing the value from being corrupted
	//    by a concurrent Set/releaseItem operation (which was the original data race).
	value := obj.value
	s.mu.RUnlock() // Unlock after reading the value

	return value, true
}

// GetOrSet is an atomic Get-or-Set operation, suitable for scenarios that require
// computation of default values (like DNS). Returns (value, wasPresent).
func (c *Cache) GetOrSet(key string, computeValue func() (interface{}, time.Duration)) (interface{}, bool) {
	// First attempt to read (fast path)
	s := c.getShard(key)
	now := time.Now().UnixNano()

	s.mu.RLock()
	obj, ok := s.items[key]
	if ok && (obj.expiry == 0 || obj.expiry > now) {
		value := obj.value
		s.mu.RUnlock()
		return value, true // Cache hit
	}
	s.mu.RUnlock()

	// Cache miss, upgrade to write lock (slow path)
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double check (another goroutine might have written)
	obj, ok = s.items[key]
	now = time.Now().UnixNano()
	if ok && (obj.expiry == 0 || obj.expiry > now) {
		return obj.value, true
	}

	// If an expired item exists, release it
	if ok {
		releaseItem(obj)
	}

	// Compute new value
	value, ttl := computeValue()

	var expiry int64
	if ttl > 0 {
		expiry = time.Now().Add(ttl).UnixNano()
	}

	s.items[key] = acquireItem(value, expiry)
	return value, false // Newly computed value
}

// SetNX is an alias for SetIfNotExists.
func (c *Cache) SetNX(key string, value interface{}, ttl time.Duration) bool {
	return c.SetIfNotExists(key, value, ttl)
}

// Del deletes a cache entry.
func (c *Cache) Del(key string) {
	s := c.getShard(key)
	s.mu.Lock()
	if item, ok := s.items[key]; ok {
		delete(s.items, key)
		releaseItem(item)
	}
	s.mu.Unlock()
}

// Clear removes all items from the cache.
func (c *Cache) Clear() {
	for _, s := range c.shards {
		s.mu.Lock()
		// Release all items
		for _, item := range s.items {
			releaseItem(item)
		}
		s.items = make(map[string]*item)
		s.mu.Unlock()
	}
}

// SetBatch sets multiple entries efficiently, reducing lock contention for bulk operations.
func (c *Cache) SetBatch(entries map[string]struct {
	Value interface{}
	TTL   time.Duration
}) {
	// Group by shard
	shardGroups := make([]map[string]*item, len(c.shards))
	for i := range shardGroups {
		shardGroups[i] = make(map[string]*item)
	}

	now := time.Now()
	for key, entry := range entries {
		var expiry int64
		if entry.TTL > 0 {
			expiry = now.Add(entry.TTL).UnixNano()
		}

		hash := fnv32a(key)
		shardIdx := hash & c.shardMask
		shardGroups[shardIdx][key] = acquireItem(entry.Value, expiry)
	}

	// Batch write to each shard in parallel
	var wg sync.WaitGroup
	for i, s := range c.shards {
		if len(shardGroups[i]) == 0 {
			continue
		}

		wg.Add(1)
		go func(shard *shard, items map[string]*item) {
			defer wg.Done()
			shard.mu.Lock()
			// Release old items that are being overwritten
			for k, newItem := range items {
				if oldItem, exists := shard.items[k]; exists {
					releaseItem(oldItem)
				}
				shard.items[k] = newItem
			}
			shard.mu.Unlock()
		}(s, shardGroups[i])
	}
	wg.Wait()
}

// cleanup periodically deletes expired items.
func (c *Cache) cleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.deleteExpired()
		case <-c.stop:
			return
		}
	}
}

// EvictedTotal returns the total number of items evicted by the cleanup process.
func (c *Cache) EvictedTotal() uint64 {
	return c.evictedTotal.Load()
}

// deleteExpired deletes all expired items using a worker pool.
func (c *Cache) deleteExpired() {
	now := time.Now().UnixNano()

	// Use a limited number of workers to process all shards
	workerCount := 8
	if len(c.shards) < workerCount {
		workerCount = len(c.shards)
	}

	shardChan := make(chan uint32, len(c.shards))
	var wg sync.WaitGroup
	var totalEvicted atomic.Uint64

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localEvicted := 0

			for shardIdx := range shardChan {
				s := c.shards[shardIdx]

				s.mu.Lock()
				for key, item := range s.items {
					if item.expiry > 0 && item.expiry < now {
						delete(s.items, key)
						releaseItem(item) // Release the expired item
						localEvicted++
					}
				}
				s.mu.Unlock()
			}

			if localEvicted > 0 {
				totalEvicted.Add(uint64(localEvicted))
			}
		}()
	}

	// Distribute tasks
	for i := uint32(0); i < uint32(len(c.shards)); i++ {
		shardChan <- i
	}
	close(shardChan)

	wg.Wait()

	evicted := totalEvicted.Load()
	if evicted > 0 {
		c.evictedTotal.Add(evicted)
	}
}

// Stop stops the background cleanup routine.
func (c *Cache) Stop() {
	close(c.stop)
}

// Save saves the cache to a file using a custom serializer.
func (c *Cache) Save(filename string, serializer Serializer) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	now := time.Now().UnixNano()

	// Write magic number and version (little-endian)
	if err := binary.Write(file, binary.LittleEndian, uint32(0x43414348)); err != nil { // "CACH"
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(1)); err != nil { // version 1
		return err
	}

	// Write evictedTotal
	if err := binary.Write(file, binary.LittleEndian, c.evictedTotal.Load()); err != nil {
		return err
	}

	// Count the number of valid items across all shards
	validCount := 0
	for _, s := range c.shards {
		s.mu.RLock()
		for _, item := range s.items {
			if item.expiry == 0 || item.expiry > now {
				validCount++
			}
		}
		s.mu.RUnlock()
	}

	// Write the count of valid items
	if err := binary.Write(file, binary.LittleEndian, uint32(validCount)); err != nil {
		return err
	}

	// Write each cache item
	for _, s := range c.shards {
		s.mu.RLock()
		for key, item := range s.items {
			// Skip expired items
			if item.expiry > 0 && item.expiry < now {
				continue
			}

			// Serialize the value
			valueBytes, err := serializer.Serialize(item.value)
			if err != nil {
				// Skip non-serializable items
				continue
			}

			// Write key length and content
			if err := binary.Write(file, binary.LittleEndian, uint32(len(key))); err != nil {
				s.mu.RUnlock()
				return err
			}
			if _, err := file.Write([]byte(key)); err != nil {
				s.mu.RUnlock()
				return err
			}

			// Write value length and content
			if err := binary.Write(file, binary.LittleEndian, uint32(len(valueBytes))); err != nil {
				s.mu.RUnlock()
				return err
			}
			if _, err := file.Write(valueBytes); err != nil {
				s.mu.RUnlock()
				return err
			}

			// Write expiry time
			if err := binary.Write(file, binary.LittleEndian, item.expiry); err != nil {
				s.mu.RUnlock()
				return err
			}
		}
		s.mu.RUnlock()
	}

	return nil
}

// Load loads the cache from a file using a custom serializer.
func (c *Cache) Load(filename string, serializer Serializer) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Read and validate magic number
	var magic uint32
	if err := binary.Read(file, binary.LittleEndian, &magic); err != nil {
		return err
	}
	if magic != 0x43414348 { // "CACH"
		return errors.New("invalid cache file format")
	}

	// Read version
	var version uint32
	if err := binary.Read(file, binary.LittleEndian, &version); err != nil {
		return err
	}
	if version != 1 {
		return errors.New("unsupported cache file version")
	}

	// Read evictedTotal
	var evictedTotal uint64
	if err := binary.Read(file, binary.LittleEndian, &evictedTotal); err != nil {
		return err
	}

	// Read item count
	var count uint32
	if err := binary.Read(file, binary.LittleEndian, &count); err != nil {
		return err
	}

	// Temporary storage for all read items
	tempItems := make(map[string]*item, count)
	now := time.Now().UnixNano()

	// Read each cache item
	for i := uint32(0); i < count; i++ {
		// Read key
		var keyLen uint32
		if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			return err
		}
		key := string(keyBytes)

		// Read value
		var valueLen uint32
		if err := binary.Read(file, binary.LittleEndian, &valueLen); err != nil {
			return err
		}
		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(file, valueBytes); err != nil {
			return err
		}

		// Deserialize value
		value, err := serializer.Deserialize(valueBytes)
		if err != nil {
			// Skip unserializable items, but still read expiry to maintain file pointer position
			var expiryNano int64
			if err := binary.Read(file, binary.LittleEndian, &expiryNano); err != nil {
				return err
			}
			continue
		}

		// Read expiry time
		var expiryNano int64
		if err := binary.Read(file, binary.LittleEndian, &expiryNano); err != nil {
			return err
		}

		// Skip expired items
		if expiryNano > 0 && expiryNano < now {
			continue
		}

		// Acquire item from pool
		tempItems[key] = acquireItem(value, expiryNano)
	}

	// Distribute data to shards
	// Clear all shards first (release old items)
	for _, s := range c.shards {
		s.mu.Lock()
		for _, item := range s.items {
			releaseItem(item)
		}
		s.items = make(map[string]*item)
		s.mu.Unlock()
	}

	// Bulk allocate to corresponding shards, reducing lock operations
	shardData := make([]map[string]*item, len(c.shards))
	for i := range shardData {
		shardData[i] = make(map[string]*item)
	}

	// Group first
	for key, item := range tempItems {
		hash := fnv32a(key)
		shardIdx := hash & c.shardMask
		shardData[shardIdx][key] = item
	}

	// Then bulk write to each shard
	for i, s := range c.shards {
		if len(shardData[i]) > 0 {
			s.mu.Lock()
			s.items = shardData[i]
			s.mu.Unlock()
		}
	}

	c.evictedTotal.Store(evictedTotal)

	return nil
}

// fnv32a FNV-1a Hash implementation
func fnv32a(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	hash := uint32(offset32)
	for i := 0; i < len(s); i++ {
		hash ^= uint32(s[i])
		hash *= prime32
	}
	return hash
}