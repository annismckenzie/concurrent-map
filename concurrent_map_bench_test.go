package cmap

import (
	"strconv"
	"sync"
	"testing"
)

type protectedMapper interface {
	Get(key string) (interface{}, bool)
	Set(key string, value interface{})
	Items() map[string]interface{}
}

var benchmarkShardCounts = []int{0, 1, 16, 32, 256}

type benchmarkDefinition struct {
	NonParallelFunc func(b *testing.B, m protectedMapper)
	ParallelFunc    func(b *testing.B, m protectedMapper, Parallelism int)
	Parallelism     int
}

func BenchmarkAll(b *testing.B) {
	for name, benchmark := range benchmarks {
		for _, benchmarkShardCount := range benchmarkShardCounts {
			if benchmarkShardCount == 0 {
				b.Run(name+",Shards=0,Parallel=no", func(b *testing.B) { benchmark.NonParallelFunc(b, newMutexProtectedMap()) })
				b.Run(name+",Shards=0,Parallel=yes", func(b *testing.B) { benchmark.ParallelFunc(b, newMutexProtectedMap(), benchmark.Parallelism) })
			} else {
				b.Run(name+",Shards="+strconv.Itoa(benchmarkShardCount)+",Parallel=no", func(b *testing.B) { benchmark.NonParallelFunc(b, New(benchmarkShardCount)) })
				b.Run(name+",Shards="+strconv.Itoa(benchmarkShardCount)+",Parallel=yes", func(b *testing.B) { benchmark.ParallelFunc(b, New(benchmarkShardCount), benchmark.Parallelism) })
			}
		}
	}
}

func BenchmarkHighParallelism(b *testing.B) {
	var (
		shardCount  = 32
		name        = "MultiGetSetBlockEveryPromilleParallelism1000"
		parallelism = 1000
	)
	b.Run(name+",Shards="+strconv.Itoa(shardCount)+",Parallel=yes", func(b *testing.B) {
		benchmarkMultiGetSetBlockEveryPromilleConcurrently(b, New(shardCount), parallelism)
	})
}

var benchmarks = map[string]benchmarkDefinition{
	"SingleInsertAbsent":   benchmarkDefinition{NonParallelFunc: benchmarkSingleInsertAbsent, ParallelFunc: benchmarkSingleInsertAbsentConcurrently, Parallelism: 100},
	"SingleInsertPresent":  benchmarkDefinition{NonParallelFunc: benchmarkSingleInsertPresent, ParallelFunc: benchmarkSingleInsertPresentConcurrently, Parallelism: 100},
	"MultiInsertDifferent": benchmarkDefinition{NonParallelFunc: benchmarkMultiInsertDifferent, ParallelFunc: benchmarkMultiInsertDifferentConcurrently, Parallelism: 100},
	"MultiGetSetDifferent": benchmarkDefinition{NonParallelFunc: benchmarkMultiGetSetDifferent, ParallelFunc: benchmarkMultiGetSetDifferentConcurrently, Parallelism: 100},
	"MultiGetSetBlock":     benchmarkDefinition{NonParallelFunc: benchmarkMultiGetSetBlock, ParallelFunc: benchmarkMultiGetSetBlockConcurrently, Parallelism: 100},
	"Items":                benchmarkDefinition{NonParallelFunc: benchmarkItems, ParallelFunc: benchmarkItemsConcurrently, Parallelism: 100},

	"MultiGetSetBlockParallelism1000": benchmarkDefinition{NonParallelFunc: benchmarkMultiGetSetBlock, ParallelFunc: benchmarkMultiGetSetBlockConcurrently, Parallelism: 1000},
}

func benchmarkItems(b *testing.B, m protectedMapper) {
	for i := 0; i < 10000; i++ {
		m.Set(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Items()
	}
}

func benchmarkItemsConcurrently(b *testing.B, m protectedMapper, parallelism int) {
	for i := 0; i < 10000; i++ {
		m.Set(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}
	b.SetParallelism(parallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Items()
		}
	})
}

func benchmarkSingleInsertAbsent(b *testing.B, m protectedMapper) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(strconv.Itoa(i), "value")
	}
}

func benchmarkSingleInsertAbsentConcurrently(b *testing.B, m protectedMapper, parallelism int) {
	keys := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = i
	}
	b.SetParallelism(parallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Set(strconv.Itoa(keys[i]), "value")
			i++
		}
	})
}

func benchmarkSingleInsertPresent(b *testing.B, m protectedMapper) {
	m.Set("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set("key", "value")
	}
}

func benchmarkSingleInsertPresentConcurrently(b *testing.B, m protectedMapper, parallelism int) {
	m.Set("key", "value")
	b.SetParallelism(parallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Set("key", "value")
		}
	})
}

func benchmarkMultiInsertDifferent(b *testing.B, m protectedMapper) {
	finished := make(chan struct{}, b.N)
	_, set := getSet(m, finished)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set(strconv.Itoa(i), "value")
	}
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

func benchmarkMultiInsertDifferentConcurrently(b *testing.B, m protectedMapper, parallelism int) {
	finished := make(chan struct{}, b.N)
	_, set := getSet(m, finished)
	keys := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = i
	}
	b.SetParallelism(parallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			set(strconv.Itoa(keys[i]), "value")
			i++
		}
	})
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

func benchmarkMultiGetSetDifferent(b *testing.B, m protectedMapper) {
	finished := make(chan struct{}, 2*b.N)
	get, set := getSet(m, finished)
	m.Set("-1", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set(strconv.Itoa(i-1), "value")
		get(strconv.Itoa(i), "value")
	}
	b.StopTimer()
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

func benchmarkMultiGetSetDifferentConcurrently(b *testing.B, m protectedMapper, parallelism int) {
	finished := make(chan struct{}, 2*b.N)
	get, set := getSet(m, finished)
	m.Set("-1", "value")
	keys := make([]int, 2*b.N+1)
	keys[0] = -1
	for i := 1; i <= 2*b.N; i++ {
		keys[i] = i
	}
	b.SetParallelism(parallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			set(strconv.Itoa(keys[i]), "value")
			get(strconv.Itoa(keys[i+1]), "value")
			i++
		}
	})
	b.StopTimer()
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

func benchmarkMultiGetSetBlock(b *testing.B, m protectedMapper) {
	finished := make(chan struct{}, 2*b.N)
	get, set := getSet(m, finished)
	for i := 0; i < b.N; i++ {
		m.Set(strconv.Itoa(i%100), "value")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set(strconv.Itoa(i%100), "value")
		get(strconv.Itoa(i%100), "value")
	}
	b.StopTimer()
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

func benchmarkMultiGetSetBlockConcurrently(b *testing.B, m protectedMapper, parallelism int) {
	finished := make(chan struct{}, 2*b.N)
	get, set := getSet(m, finished)
	for i := 0; i < b.N; i++ {
		m.Set(strconv.Itoa(i%100), "value")
	}
	b.SetParallelism(parallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			set(strconv.Itoa(i%100), "value")
			get(strconv.Itoa(i%100), "value")
			i++
		}
	})
	b.StopTimer()
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

func benchmarkMultiGetSetBlockEveryPromilleConcurrently(b *testing.B, m protectedMapper, parallelism int) {
	finished := make(chan struct{}, 2*b.N)
	get, set := getSet(m, finished)
	for i := 0; i < b.N; i++ {
		m.Set(strconv.Itoa(i%100), "value")
	}
	b.SetParallelism(parallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%parallelism == 0 {
				set(strconv.Itoa(i%100), "value")
			} else {
				finished <- struct{}{}
			}
			get(strconv.Itoa(i%100), "value")
			i++
		}
	})
	b.StopTimer()
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

func BenchmarkMarshalJSON(b *testing.B) {
	m := New()

	// insert 10000 elements
	for i := 0; i < 10000; i++ {
		m.Set(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.MarshalJSON()
	}
}

func BenchmarkStrconv(b *testing.B) {
	for i := 0; i < b.N; i++ {
		strconv.Itoa(i)
	}
}

func BenchmarkMultiInsertSame(b *testing.B) {
	m := New()
	finished := make(chan struct{}, b.N)
	_, set := getSet(m, finished)
	m.Set("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set("key", "value")
	}
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

func BenchmarkMultiGetSame(b *testing.B) {
	m := New()
	finished := make(chan struct{}, b.N)
	get, _ := getSet(m, finished)
	m.Set("key", "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		get("key", "value")
	}
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

func BenchmarkKeys(b *testing.B) {
	m := New()

	// insert 10000 elements
	for i := 0; i < 10000; i++ {
		m.Set(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Keys()
	}
}

func getSet(m protectedMapper, finished chan struct{}) (set func(key, value string), get func(key, value string)) {
	get = func(key, value string) {
		for i := 0; i < 10; i++ {
			m.Get(key)
		}
		finished <- struct{}{}
	}
	set = func(key, value string) {
		for i := 0; i < 10; i++ {
			m.Set(key, value)
		}
		finished <- struct{}{}
	}
	return
}

type mutexProtectedMap struct {
	items map[string]interface{}
	sync.RWMutex
}

func newMutexProtectedMap() *mutexProtectedMap {
	return &mutexProtectedMap{items: make(map[string]interface{})}
}

func (m *mutexProtectedMap) Get(key string) (interface{}, bool) {
	m.RLock()
	val, ok := m.items[key]
	m.RUnlock()
	return val, ok
}

func (m *mutexProtectedMap) Set(key string, value interface{}) {
	m.Lock()
	m.items[key] = value
	m.Unlock()
}

func (m *mutexProtectedMap) Items() map[string]interface{} {
	tmp := make(map[string]interface{})

	for k, v := range m.items {
		tmp[k] = v
	}

	return tmp
}
