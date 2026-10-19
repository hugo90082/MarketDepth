package main

import (
	"sort"
	"sync"
	"time"
)

// RecentFuturesBuffer holds the most recent one-second Binance USDⓈ-M
// trade-price rows. Historical futures chunks continue to be written
// unchanged by the normal storage pipeline.
type RecentFuturesBuffer struct {
	mu     sync.RWMutex
	window time.Duration
	rows   []FuturesSnapshot
}

func NewRecentFuturesBuffer(window time.Duration) *RecentFuturesBuffer {
	if window <= 0 {
		window = recentBufferDuration
	}
	return &RecentFuturesBuffer{window: window, rows: []FuturesSnapshot{}}
}

func (b *RecentFuturesBuffer) Add(row FuturesSnapshot) {
	b.mu.Lock()
	defer b.mu.Unlock()
	i := sort.Search(len(b.rows), func(i int) bool { return b.rows[i].T >= row.T })
	if i < len(b.rows) && b.rows[i].T == row.T {
		b.rows[i] = row
	} else {
		b.rows = append(b.rows, FuturesSnapshot{})
		copy(b.rows[i+1:], b.rows[i:])
		b.rows[i] = row
	}
	cutoff := row.T - b.window.Milliseconds()
	n := sort.Search(len(b.rows), func(i int) bool { return b.rows[i].T > cutoff })
	if n > 0 {
		b.rows = b.rows[n:]
	}
}

func (b *RecentFuturesBuffer) RowsSince(since int64) (rows []FuturesSnapshot, oldest, latest int64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	rows = []FuturesSnapshot{}
	if len(b.rows) == 0 {
		return rows, 0, 0
	}
	oldest = b.rows[0].T
	latest = b.rows[len(b.rows)-1].T
	i := sort.Search(len(b.rows), func(i int) bool { return b.rows[i].T > since })
	return append(rows, b.rows[i:]...), oldest, latest
}
