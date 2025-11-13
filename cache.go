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

// Serializer 序列化接口，让使用者自己实现具体类型的序列化
type Serializer interface {
	// Serialize 将 value 序列化为字节数组
	Serialize(value interface{}) ([]byte, error)
	// Deserialize 从字节数组反序列化为具体类型
	Deserialize(data []byte) (interface{}, error)
}

// item 内部使用，不导出
type item struct {
	value  interface{}
	expiry int64 // 改用 Unix 纳秒时间戳，避免 time.Time 的开销
}

// itemPool 对象池，用于复用 item 对象，减少 GC 压力
var itemPool = sync.Pool{
	New: func() interface{} {
		return &item{}
	},
}

// acquireItem 从池中获取并初始化 item
func acquireItem(value interface{}, expiry int64) *item {
	i := itemPool.Get().(*item)
	i.value = value
	i.expiry = expiry
	return i
}

// releaseItem 将 item 归还到池中，并清理字段
func releaseItem(i *item) {
	// 清理字段，特别是 interface{} (value)，帮助 GC
	i.value = nil
	i.expiry = 0
	itemPool.Put(i)
}

// 提供方法访问字段（如果需要的话）
func (i *item) Value() interface{} {
	return i.value
}

func (i *item) Expiry() time.Time {
	if i.expiry == 0 {
		return time.Time{}
	}
	return time.Unix(0, i.expiry)
}

func (i *item) IsExpired() bool {
	return i.expiry > 0 && i.expiry < time.Now().UnixNano()
}

// shard 分片结构
type shard struct {
	mu    sync.RWMutex
	items map[string]*item
	_     [40]byte // 缓存行填充，避免伪共享
}

// Cache 分片缓存结构
type Cache struct {
	shards       []*shard
	shardMask    uint32 // 使用位掩码代替取模运算
	stop         chan struct{}
	evictedTotal atomic.Uint64
}

// New 创建新的缓存实例（默认256分片）
func New(cleanupInterval time.Duration) *Cache {
	return NewWithShardCount(cleanupInterval, 256)
}

// NewWithShardCount 创建指定分片数的缓存实例
// shardCount 必须是2的幂次方，否则会自动调整到最接近的2的幂次方
func NewWithShardCount(cleanupInterval time.Duration, shardCount uint32) *Cache {
	// 确保分片数是2的幂次方，这样可以用位运算代替取模
	if shardCount == 0 {
		shardCount = 256
	}
	shardCount = nextPowerOfTwo(shardCount)

	c := &Cache{
		shards:    make([]*shard, shardCount),
		shardMask: shardCount - 1, // 用于快速取模
		stop:      make(chan struct{}),
	}

	// 初始化所有分片
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

// nextPowerOfTwo 返回大于等于 n 的最小的2的幂次方
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

// getShard 根据key获取对应的分片（使用位运算优化）
func (c *Cache) getShard(key string) *shard {
	hash := fnv32a(key)
	return c.shards[hash&c.shardMask] // 位运算代替取模，更快
}

// Count 返回缓存中的项数
func (c *Cache) Count() int {
	count := 0
	for _, s := range c.shards {
		s.mu.RLock()
		count += len(s.items)
		s.mu.RUnlock()
	}
	return count
}

// Set 无条件设置缓存项（会覆盖已存在的key）
func (c *Cache) Set(key string, value interface{}, ttl time.Duration) {
	var expiry int64
	if ttl > 0 {
		expiry = time.Now().Add(ttl).UnixNano()
	}

	s := c.getShard(key)
	s.mu.Lock()

	// 如果 key 已存在，先释放旧的 item
	if oldItem, exists := s.items[key]; exists {
		releaseItem(oldItem)
	}

	// 从池中获取新的 item
	s.items[key] = acquireItem(value, expiry)
	s.mu.Unlock()
}

// Exists 检查key是否存在且未过期
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

// SetIfNotExists 仅在key不存在时设置（更安全的选择）
func (c *Cache) SetIfNotExists(key string, value interface{}, ttl time.Duration) bool {
	s := c.getShard(key)
	nowNano := time.Now().UnixNano() // <--- [修正 1] 只调用一次时间

	// --- 1. 快速路径：只读锁检查 ---
	s.mu.RLock()
	obj, exists := s.items[key]
	if exists {
		// [修正 2] 使用外部时间戳检查
		if obj.expiry == 0 || obj.expiry > nowNano {
			s.mu.RUnlock()
			return false // 存在且未过期，快速返回
		}
	}
	s.mu.RUnlock()

	// --- 2. 慢路径：需要写入（不存在或已过期）---

	// 重新计算 expiry (使用相同的 nowNano)
	var expiry int64
	if ttl > 0 {
		expiry = nowNano + ttl.Nanoseconds() // <--- 确保使用 nowNano，而不是再次调用 Now()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// --- 3. 双重检查：可能其他协程已经设置了 ---
	if objItem, ok := s.items[key]; ok {
		// [修正 3] 再次使用 nowNano 进行检查
		if objItem.expiry == 0 || objItem.expiry > nowNano {
			return false // 已存在且未过期，退出
		}
		// 过期了，释放旧的 item
		releaseItem(objItem)
	}

	// --- 4. 写入 ---
	s.items[key] = acquireItem(value, expiry)
	return true
}

// Replace 仅在key存在时替换值
func (c *Cache) Replace(key string, value interface{}, ttl time.Duration) bool {
	s := c.getShard(key)
	now := time.Now().UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	// 检查是否存在且未过期
	if itemObject, ok := s.items[key]; ok {
		if itemObject.expiry == 0 || itemObject.expiry > now {
			// 存在且未过期，释放旧的 item
			releaseItem(itemObject)

			// 进行替换
			var expiry int64
			if ttl > 0 {
				expiry = time.Now().Add(ttl).UnixNano()
			}

			s.items[key] = acquireItem(value, expiry)
			return true
		}
	}

	return false // key不存在或已过期
}

// CompareAndDelete 比较并删除
func (c *Cache) CompareAndDelete(key string, expectedValue interface{}) bool {
	s := c.getShard(key)
	now := time.Now().UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	obj, ok := s.items[key]
	if !ok {
		return false // key不存在
	}

	// 检查过期
	if obj.expiry > 0 && obj.expiry < now {
		delete(s.items, key)
		releaseItem(obj)
		return false // 已过期
	}

	// 比较值
	if obj.value == expectedValue {
		delete(s.items, key)
		releaseItem(obj)
		return true // 成功删除
	}

	return false // 值不匹配
}

// Get 获取缓存项（无统计）
/*
func (c *Cache) Get(key string) (interface{}, bool) {
	s := c.getShard(key)
	s.mu.RLock()
	obj, ok := s.items[key]
	s.mu.RUnlock()

	if !ok {
		return nil, false
	}

	// 检查过期（在锁外检查，减少锁持有时间）
	if obj.expiry > 0 && obj.expiry < time.Now().UnixNano() {
		return nil, false
	}

	return obj.value, true
}
*/

func (c *Cache) Get(key string) (interface{}, bool) {
	s := c.getShard(key)
	s.mu.RLock()
	obj, ok := s.items[key]

	if !ok {
		s.mu.RUnlock()
		return nil, false
	}

	// 1. 在锁内检查过期
	if obj.expiry > 0 && obj.expiry < time.Now().UnixNano() {
		s.mu.RUnlock()
		return nil, false
	}

	// 2. 在锁内读取 value，防止被 Set/releaseItem 污染
	value := obj.value
	s.mu.RUnlock() // 拿到 value 之后再解锁

	return value, true
}

// GetOrSet 原子性的 Get-or-Set 操作，适合 DNS 等需要计算默认值的场景
// 返回值：(value, wasPresent)
func (c *Cache) GetOrSet(key string, computeValue func() (interface{}, time.Duration)) (interface{}, bool) {
	// 先尝试读取
	s := c.getShard(key)
	now := time.Now().UnixNano()

	s.mu.RLock()
	obj, ok := s.items[key]
	if ok && (obj.expiry == 0 || obj.expiry > now) {
		value := obj.value
		s.mu.RUnlock()
		return value, true // 缓存命中
	}
	s.mu.RUnlock()

	// 缓存未命中，升级为写锁
	s.mu.Lock()
	defer s.mu.Unlock()

	// 双重检查（可能其他 goroutine 已经写入）
	obj, ok = s.items[key]
	now = time.Now().UnixNano()
	if ok && (obj.expiry == 0 || obj.expiry > now) {
		return obj.value, true
	}

	// 如果存在过期的项，释放它
	if ok {
		releaseItem(obj)
	}

	// 计算新值
	value, ttl := computeValue()

	var expiry int64
	if ttl > 0 {
		expiry = time.Now().Add(ttl).UnixNano()
	}

	s.items[key] = acquireItem(value, expiry)
	return value, false // 新计算的值
}

// SetNX Set if Not eXists
func (c *Cache) SetNX(key string, value interface{}, ttl time.Duration) bool {
	return c.SetIfNotExists(key, value, ttl)
}

// Del 删除缓存项
func (c *Cache) Del(key string) {
	s := c.getShard(key)
	s.mu.Lock()
	if item, ok := s.items[key]; ok {
		delete(s.items, key)
		releaseItem(item)
	}
	s.mu.Unlock()
}

// Clear 清空缓存中的所有项
func (c *Cache) Clear() {
	for _, s := range c.shards {
		s.mu.Lock()
		// 释放所有 item
		for _, item := range s.items {
			releaseItem(item)
		}
		s.items = make(map[string]*item)
		s.mu.Unlock()
	}
}

// SetBatch 批量设置，减少锁竞争，适合预热场景
func (c *Cache) SetBatch(entries map[string]struct {
	Value interface{}
	TTL   time.Duration
}) {
	// 按分片分组
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

	// 批量写入各分片
	var wg sync.WaitGroup
	for i, s := range c.shards {
		if len(shardGroups[i]) == 0 {
			continue
		}

		wg.Add(1)
		go func(shard *shard, items map[string]*item) {
			defer wg.Done()
			shard.mu.Lock()
			// 释放被覆盖的旧 item
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

// cleanup 定期清理过期项
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

// EvictedTotal 返回已驱逐的项总数
func (c *Cache) EvictedTotal() uint64 {
	return c.evictedTotal.Load()
}

// deleteExpired 删除所有过期项（优化版本：使用工作池避免创建过多goroutine）
func (c *Cache) deleteExpired() {
	now := time.Now().UnixNano()

	// 使用有限数量的 worker 来处理所有分片
	workerCount := 8
	if len(c.shards) < workerCount {
		workerCount = len(c.shards)
	}

	shardChan := make(chan uint32, len(c.shards))
	var wg sync.WaitGroup
	var totalEvicted atomic.Uint64

	// 启动 worker
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
						releaseItem(item) // 释放过期的 item
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

	// 分配任务
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

// Stop 停止后台清理
func (c *Cache) Stop() {
	close(c.stop)
}

// Save 使用自定义序列化器保存缓存到文件
func (c *Cache) Save(filename string, serializer Serializer) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	now := time.Now().UnixNano()

	// 写入魔数和版本号（小端序）
	if err := binary.Write(file, binary.LittleEndian, uint32(0x43414348)); err != nil { // "CACH"
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(1)); err != nil { // version 1
		return err
	}

	// 写入 evictedTotal
	if err := binary.Write(file, binary.LittleEndian, c.evictedTotal.Load()); err != nil {
		return err
	}

	// 计算所有分片中的有效项数量
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

	// 写入有效项数量
	if err := binary.Write(file, binary.LittleEndian, uint32(validCount)); err != nil {
		return err
	}

	// 写入每个缓存项
	for _, s := range c.shards {
		s.mu.RLock()
		for key, item := range s.items {
			// 跳过已过期的项
			if item.expiry > 0 && item.expiry < now {
				continue
			}

			// 序列化值
			valueBytes, err := serializer.Serialize(item.value)
			if err != nil {
				// 跳过无法序列化的项
				continue
			}

			// 写入 key 长度和内容
			if err := binary.Write(file, binary.LittleEndian, uint32(len(key))); err != nil {
				s.mu.RUnlock()
				return err
			}
			if _, err := file.Write([]byte(key)); err != nil {
				s.mu.RUnlock()
				return err
			}

			// 写入 value 长度和内容
			if err := binary.Write(file, binary.LittleEndian, uint32(len(valueBytes))); err != nil {
				s.mu.RUnlock()
				return err
			}
			if _, err := file.Write(valueBytes); err != nil {
				s.mu.RUnlock()
				return err
			}

			// 写入过期时间
			if err := binary.Write(file, binary.LittleEndian, item.expiry); err != nil {
				s.mu.RUnlock()
				return err
			}
		}
		s.mu.RUnlock()
	}

	return nil
}

// Load 使用自定义序列化器从文件加载缓存
func (c *Cache) Load(filename string, serializer Serializer) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// 读取并验证魔数
	var magic uint32
	if err := binary.Read(file, binary.LittleEndian, &magic); err != nil {
		return err
	}
	if magic != 0x43414348 { // "CACH"
		return errors.New("invalid cache file format")
	}

	// 读取版本号
	var version uint32
	if err := binary.Read(file, binary.LittleEndian, &version); err != nil {
		return err
	}
	if version != 1 {
		return errors.New("unsupported cache file version")
	}

	// 读取 evictedTotal
	var evictedTotal uint64
	if err := binary.Read(file, binary.LittleEndian, &evictedTotal); err != nil {
		return err
	}

	// 读取项数量
	var count uint32
	if err := binary.Read(file, binary.LittleEndian, &count); err != nil {
		return err
	}

	// 临时存储所有读取的项
	tempItems := make(map[string]*item, count)
	now := time.Now().UnixNano()

	// 读取每个缓存项
	for i := uint32(0); i < count; i++ {
		// 读取 key
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

		// 读取 value
		var valueLen uint32
		if err := binary.Read(file, binary.LittleEndian, &valueLen); err != nil {
			return err
		}
		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(file, valueBytes); err != nil {
			return err
		}

		// 反序列化值
		value, err := serializer.Deserialize(valueBytes)
		if err != nil {
			// 跳过无法反序列化的项
			// 但仍需读取过期时间以保持文件指针位置正确
			var expiryNano int64
			if err := binary.Read(file, binary.LittleEndian, &expiryNano); err != nil {
				return err
			}
			continue
		}

		// 读取过期时间
		var expiryNano int64
		if err := binary.Read(file, binary.LittleEndian, &expiryNano); err != nil {
			return err
		}

		// 跳过已过期的项
		if expiryNano > 0 && expiryNano < now {
			continue
		}

		// 使用池中的 item
		tempItems[key] = acquireItem(value, expiryNano)
	}

	// 将数据分配到各个分片中
	// 先清空所有分片（释放旧的 item）
	for _, s := range c.shards {
		s.mu.Lock()
		for _, item := range s.items {
			releaseItem(item)
		}
		s.items = make(map[string]*item)
		s.mu.Unlock()
	}

	// 批量分配到对应分片，减少锁操作
	shardData := make([]map[string]*item, len(c.shards))
	for i := range shardData {
		shardData[i] = make(map[string]*item)
	}

	// 先分组
	for key, item := range tempItems {
		hash := fnv32a(key)
		shardIdx := hash & c.shardMask
		shardData[shardIdx][key] = item
	}

	// 再批量写入各分片
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

// fnv32a FNV-1a 哈希算法实现
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
