package normalize

import (
	"sync"
	"testing"
	"time"
)

func TestDedupDetectsDuplicates(t *testing.T) {
	d := NewDedup(time.Minute, 1000)
	if d.Seen("binance", "BTCUSDT", 1) {
		t.Fatal("first sighting must not be a duplicate")
	}
	if !d.Seen("binance", "BTCUSDT", 1) {
		t.Error("second sighting must be a duplicate")
	}
	if d.Seen("binance", "BTCUSDT", 2) {
		t.Error("a different trade id is not a duplicate")
	}
	if d.Seen("binance", "ETHUSDT", 1) {
		t.Error("the same id on another symbol is not a duplicate")
	}
	if d.Seen("coinbase", "BTCUSDT", 1) {
		t.Error("the same id on another exchange is not a duplicate")
	}
	if got := d.Len(); got != 4 {
		t.Errorf("Len() = %d, want 4", got)
	}
}

func TestDedupExpiresEntries(t *testing.T) {
	d := NewDedup(50*time.Millisecond, 1000)
	now := time.Now()
	d.now = func() time.Time { return now }

	if d.Seen("binance", "BTCUSDT", 7) {
		t.Fatal("unexpected duplicate")
	}
	now = now.Add(60 * time.Millisecond)
	if d.Seen("binance", "BTCUSDT", 7) {
		t.Error("entry should have expired after the TTL")
	}
}

func TestDedupBoundsMemory(t *testing.T) {
	d := NewDedup(time.Hour, 640) // 10 keys per shard
	for i := int64(0); i < 50_000; i++ {
		d.Seen("binance", "BTCUSDT", i)
	}
	if got := d.Len(); got > 640 {
		t.Errorf("Len() = %d, want <= 640 (maxKeys)", got)
	}
}

func TestDedupReset(t *testing.T) {
	d := NewDedup(time.Minute, 1000)
	d.Seen("binance", "BTCUSDT", 1)
	d.Reset()
	if d.Seen("binance", "BTCUSDT", 1) {
		t.Error("Reset() should clear all keys")
	}
	if d.Len() != 1 { // the call above re-inserted the key
		t.Errorf("Len() = %d, want 1", d.Len())
	}
}

func TestDedupConcurrent(t *testing.T) {
	d := NewDedup(time.Minute, 100_000)
	var wg sync.WaitGroup
	const goroutines, iterations = 8, 500
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := int64(0); i < iterations; i++ {
				d.Seen("binance", "BTCUSDT", i)
			}
		}()
	}
	wg.Wait()
	// Every id is seen 8 times: the first is unique, the rest are duplicates.
	if got := d.Len(); got != iterations {
		t.Errorf("Len() = %d, want %d", got, iterations)
	}
}
