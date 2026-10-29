package lru

import (
	"sync"
	"testing"
	"time"
)

func TestCacheLRUConditionalMutations(t *testing.T) {
	c := New[string, int](WithAge[string, int](600), WithStale[string, int](true))
	if c.DeleteIf("absent", func(int) bool { return true }) {
		t.Fatal("deleted absent entry")
	}
	if c.SetIf("key", 1, func(old int, expires time.Time, found bool) bool { return found }) {
		t.Fatal("rejected missing write stored")
	}
	if !c.SetIf("key", 1, func(old int, expires time.Time, found bool) bool { return !found && old == 0 && expires.IsZero() }) {
		t.Fatal("missing first write rejected")
	}
	expires := time.Now().Add(-time.Hour).Truncate(time.Second)
	c.SetWithExpire("key", 1, expires)
	if c.SetIf("key", 2, func(old int, e time.Time, found bool) bool { return old != 1 }) {
		t.Fatal("rejected replacement stored")
	}
	if _, e, _ := c.GetWithExpire("key"); !e.Equal(expires) {
		t.Fatal("rejected write renewed TTL")
	}
	if c.DeleteIf("key", func(old int) bool { return old != 1 }) {
		t.Fatal("rejected delete removed entry")
	}
	if !c.SetIf("key", 2, func(old int, e time.Time, found bool) bool { return found && old == 1 && e.Equal(expires) }) {
		t.Fatal("matching replacement failed")
	}
	if !c.DeleteIf("key", func(old int) bool { return old == 2 }) {
		t.Fatal("matching delete failed")
	}
}

func TestCacheLRUConditionalFirstWriterConcurrent(t *testing.T) {
	c := New[string, int](WithAge[string, int](600))
	start := make(chan struct{})
	var wg sync.WaitGroup
	wins := make(chan int, 32)
	for i := 1; i <= 32; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			if c.SetIf("key", n, func(_ int, _ time.Time, found bool) bool { return !found }) {
				wins <- n
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(wins)
	winner, count := 0, 0
	for n := range wins {
		winner = n
		count++
	}
	if count != 1 {
		t.Fatalf("first writers=%d", count)
	}
	if actual, ok := c.Get("key"); !ok || actual != winner {
		t.Fatal("winning write overwritten")
	}
}
