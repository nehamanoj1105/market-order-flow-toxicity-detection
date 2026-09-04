package normalize

import (
	"hash/fnv"
	"strconv"
	"sync"
	"time"
)

// shardCount spreads the dedup map across locks. 64 is plenty: contention only
// matters at the millions-of-events-per-second end, and it keeps memory tidy.
const shardCount = 64

type shard struct {
	mu        sync.Mutex
	entries   map[uint64]int64 // hash -> expiry (unix ms)
	lastSweep time.Time
}

// Dedup suppresses duplicate (exchange, symbol, trade_id) triples for a bounded
// window. Exchanges do re-deliver: Binance's websocket can repeat an aggregate
// trade after a reconnect, and our own REST backfill deliberately overlaps the
// live stream to close gaps. Counting those twice would bias the toxicity
// statistics, so they are filtered here, at the edge.
//
// It is an in-process, best-effort filter: it survives restarts by design (the
// alternative, a shared store, would cost more than the duplicates do).
type Dedup struct {
	shards  [shardCount]*shard
	ttl     time.Duration
	maxKeys int
	now     func() time.Time
}

// NewDedup builds a duplicate filter retaining up to maxKeys entries for ttl.
func NewDedup(ttl time.Duration, maxKeys int) *Dedup {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	if maxKeys <= 0 {
		maxKeys = 1_000_000
	}
	d := &Dedup{ttl: ttl, maxKeys: maxKeys, now: time.Now}
	for i := range d.shards {
		d.shards[i] = &shard{entries: make(map[uint64]int64), lastSweep: time.Now()}
	}
	return d
}

// Seen reports whether the triple was already observed. It records the triple
// as a side effect, so it must be called exactly once per candidate event.
func (d *Dedup) Seen(exchange, symbol string, tradeID int64) bool {
	key := hashKey(exchange, symbol, tradeID)
	s := d.shards[key%shardCount]
	now := d.now().UnixMilli()
	expiry := now + d.ttl.Milliseconds()

	s.mu.Lock()
	defer s.mu.Unlock()

	if exp, ok := s.entries[key]; ok && exp > now {
		return true
	}
	s.entries[key] = expiry

	perShard := d.maxKeys / shardCount
	if perShard < 1 {
		perShard = 1
	}
	if len(s.entries) > perShard {
		s.sweepLocked(now)
		// Still over budget: drop the whole shard. Losing recent keys costs us a
		// handful of duplicate deliveries; unbounded memory would cost the node.
		if len(s.entries) > perShard {
			s.entries = make(map[uint64]int64, perShard)
		}
	}
	return false
}

// Len returns the approximate number of tracked keys.
func (d *Dedup) Len() int {
	n := 0
	for _, s := range d.shards {
		s.mu.Lock()
		n += len(s.entries)
		s.mu.Unlock()
	}
	return n
}

// Reset clears all tracked keys.
func (d *Dedup) Reset() {
	for _, s := range d.shards {
		s.mu.Lock()
		s.entries = make(map[uint64]int64)
		s.mu.Unlock()
	}
}

func (s *shard) sweepLocked(nowMS int64) {
	for k, exp := range s.entries {
		if exp <= nowMS {
			delete(s.entries, k)
		}
	}
	s.lastSweep = time.Now()
}

// hashKey folds the identity triple into a 64 bit hash.
func hashKey(exchange, symbol string, tradeID int64) uint64 {
	h := fnv.New64a()
	h.Write([]byte(exchange))
	h.Write([]byte{0})
	h.Write([]byte(symbol))
	h.Write([]byte{0})
	h.Write([]byte(strconv.FormatInt(tradeID, 10)))
	return h.Sum64()
}
