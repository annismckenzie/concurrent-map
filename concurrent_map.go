package cmap

import (
	"encoding/json"
	"fmt"
	"sync"
)

const defaultShardCount = 32

// ConcurrentMap is a thread-safe map of type string to anything.
// To avoid lock bottlenecks this map is divided into several (SHARD_COUNT) map shards.
type ConcurrentMap []*ConcurrentMapShared

// ConcurrentMapShared is a thread-safe string to anything map.
type ConcurrentMapShared struct {
	items        map[string]interface{}
	sync.RWMutex // Read Write mutex, guards access to internal map.
}

// New creates a new concurrent map.
func New(shardCount ...int) *ConcurrentMap {
	if len(shardCount) > 1 {
		panic(fmt.Sprintf("You may only pass a single integer to set number of shards to allocate, you passed %+v", shardCount))
	}
	if len(shardCount) == 0 {
		shardCount = []int{defaultShardCount}
	}
	m := make(ConcurrentMap, shardCount[0])
	for i := 0; i < shardCount[0]; i++ {
		m[i] = &ConcurrentMapShared{items: make(map[string]interface{})}
	}
	return &m
}

// GetShard returns the shard for the given key.
func (m ConcurrentMap) GetShard(key string) *ConcurrentMapShared {
	return m[uint(fnv32(key))%uint(m.ShardCount())]
}

// ShardCount returns the number of shards in the map.
func (m ConcurrentMap) ShardCount() int {
	return len(m)
}

// MSet adds multiple key value pairs at once.
func (m *ConcurrentMap) MSet(data map[string]interface{}) {
	for key, value := range data {
		shard := m.GetShard(key)
		shard.Lock()
		shard.items[key] = value
		shard.Unlock()
	}
}

// Set sets the value under the specified key.
func (m *ConcurrentMap) Set(key string, value interface{}) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

// UpsertCb is called with (key exists, old value, new value) and should return
// the new element to be inserted into the map. It is called while the write lock is held,
// therefore it MUST NOT try to access other keys in the same map; doing that will result in a deadlock.
type UpsertCb func(exist bool, valueInMap interface{}, newValue interface{}) interface{}

// Upsert inserts or updates the existing element using the provided callback function.
// The callback function can decide what to insert though (see UpsertCb function above).
func (m *ConcurrentMap) Upsert(key string, value interface{}, cb UpsertCb) (res interface{}) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// SetIfAbsent only sets the value if the key does not yet exist in the map.
func (m *ConcurrentMap) SetIfAbsent(key string, value interface{}) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

// Get retrieves the element from the map under the given key.
func (m *ConcurrentMap) Get(key string) (interface{}, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Count returns the number of elements in the map.
func (m ConcurrentMap) Count() int {
	count := 0
	shardCount := m.ShardCount()
	for i := 0; i < shardCount; i++ {
		shard := m[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Has returns whether the key exists in the map.
func (m *ConcurrentMap) Has(key string) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Remove removes an element from the map.
func (m *ConcurrentMap) Remove(key string) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// Pop removes an element from the map and returns it.
func (m *ConcurrentMap) Pop(key string) (v interface{}, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// IsEmpty returns whether the map has any elements.
func (m *ConcurrentMap) IsEmpty() bool {
	return m.Count() == 0
}

// Tuple is used by the Iter & IterBuffered functions to wrap two variables together over a channel,
type Tuple struct {
	Key string
	Val interface{}
}

// Iter returns an iterator which could be used in a for range loop.
func (m ConcurrentMap) Iter() <-chan Tuple {
	return m.IterBuffered()
}

// IterBuffered returns a buffered iterator which could be used in a for range loop.
func (m ConcurrentMap) IterBuffered() <-chan Tuple {
	ch := make(chan Tuple, m.Count())
	go func() {
		wg := sync.WaitGroup{}
		wg.Add(m.ShardCount())
		// Foreach shard.
		for _, shard := range m {
			go func(shard *ConcurrentMapShared) {
				// Foreach key, value pair.
				shard.RLock()
				for key, val := range shard.items {
					ch <- Tuple{key, val}
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()
	return ch
}

// Items returns all items as map[string]interface{}.
func (m *ConcurrentMap) Items() map[string]interface{} {
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

// IterCb is called for every key value pair found in maps. A read lock is held for all calls of a given shard,
// therefore the callback has a consistent view of its shard (does therefore not apply to the other shards).
type IterCb func(key string, v interface{})

// IterCb is a callback based iterator and the cheapest way to read all elements in a map.
func (m *ConcurrentMap) IterCb(fn IterCb) {
	for idx := range *m {
		shard := (*m)[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

// Keys returns all keys in the map as a slice of strings.
func (m ConcurrentMap) Keys() []string {
	count := m.Count()
	ch := make(chan string, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(m.ShardCount())
		for _, shard := range m {
			go func(shard *ConcurrentMapShared) {
				// Foreach key, value pair.
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// Generate keys
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = <-ch
	}
	return keys
}

// MarshalJSON can be used to marshal all items across all shards into a JSON byte slice.
// This is the only way to transform the map data into JSON because all items are held in a
// private and mutex-protected map that json.Marshal does not see.
// Note: this can be very expensive.
func (m *ConcurrentMap) MarshalJSON() ([]byte, error) {
	// create a temporary map that will hold all items spread across shards
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// Concurrent map uses Interface{} as its value, therefor JSON Unmarshal
// will probably won't know which to type to unmarshal into, in such case
// we'll end up with a value of type map[string]interface{}, In most cases this isn't
// out value type, this is why we've decided to remove this functionality.

// func (m *ConcurrentMap) UnmarshalJSON(b []byte) (err error) {
// 	// Reverse process of Marshal.

// 	tmp := make(map[string]interface{})

// 	// Unmarshal into a single map.
// 	if err := json.Unmarshal(b, &tmp); err != nil {
// 		return nil
// 	}

// 	// foreach key,value pair in temporary map insert into our concurrent map.
// 	for key, val := range tmp {
// 		m.Set(key, val)
// 	}
// 	return nil
// }
