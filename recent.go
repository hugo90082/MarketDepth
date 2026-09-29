package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	recentBufferDuration time.Duration = 16 * time.Minute
	recentStaleAfter     time.Duration = 60 * time.Second
)

type RecentBuffer struct {
	mu     sync.RWMutex
	window time.Duration
	rows   []SpotSnapshot
}

func NewRecentBuffer(window time.Duration) *RecentBuffer {
	if window <= 0 {
		window = recentBufferDuration
	}
	return &RecentBuffer{window: window, rows: []SpotSnapshot{}}
}

func (b *RecentBuffer) Add(row SpotSnapshot) {
	b.mu.Lock()
	defer b.mu.Unlock()

	i := sort.Search(len(b.rows), func(i int) bool { return b.rows[i].T >= row.T })
	if i < len(b.rows) && b.rows[i].T == row.T {
		b.rows[i] = row
	} else {
		b.rows = append(b.rows, SpotSnapshot{})
		copy(b.rows[i+1:], b.rows[i:])
		b.rows[i] = row
	}
	b.pruneLocked()
}

func (b *RecentBuffer) Replace(rows []SpotSnapshot) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(rows) == 0 {
		b.rows = []SpotSnapshot{}
		return
	}
	rows = append([]SpotSnapshot(nil), rows...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].T < rows[j].T })
	dedup := rows[:0]
	for _, row := range rows {
		if len(dedup) > 0 && dedup[len(dedup)-1].T == row.T {
			dedup[len(dedup)-1] = row
			continue
		}
		dedup = append(dedup, row)
	}
	b.rows = dedup
	b.pruneLocked()
}

func (b *RecentBuffer) pruneLocked() {
	if len(b.rows) == 0 {
		return
	}
	latest := b.rows[len(b.rows)-1].T
	cutoff := latest - b.window.Milliseconds()
	i := sort.Search(len(b.rows), func(i int) bool { return b.rows[i].T > cutoff })
	if i > 0 {
		b.rows = b.rows[i:]
	}
}

func (b *RecentBuffer) RowsSince(since int64) (rows []SpotSnapshot, oldest, latest int64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	rows = []SpotSnapshot{}
	if len(b.rows) == 0 {
		return rows, 0, 0
	}
	oldest = b.rows[0].T
	latest = b.rows[len(b.rows)-1].T
	i := sort.Search(len(b.rows), func(i int) bool { return b.rows[i].T > since })
	rows = append(rows, b.rows[i:]...)
	return rows, oldest, latest
}

// RebuildFromStore restores the recent window from both the open recovery
// buffers and sealed history chunks. Sealed rows win if the same target time
// exists in both places.
func (b *RecentBuffer) RebuildFromStore(s *Store, now int64) error {
	cutoff := now - b.window.Milliseconds()

	s.mu.Lock()
	chunks := make([]ChunkMeta, 0, 2)
	for _, c := range s.index.Chunks {
		if c.Kind == "spot" && c.EndMs > cutoff && c.StartMs <= now {
			chunks = append(chunks, c)
		}
	}
	openRows := [][]byte{}
	for _, cb := range s.buffers {
		if cb.kind != "spot" {
			continue
		}
		for _, raw := range cb.rows {
			openRows = append(openRows, append([]byte(nil), raw...))
		}
	}
	datasetDir := s.cfg.DatasetDir
	s.mu.Unlock()

	byT := map[int64]SpotSnapshot{}
	put := func(raw []byte) error {
		var row SpotSnapshot
		if err := json.Unmarshal(raw, &row); err != nil {
			return err
		}
		if row.T > cutoff && row.T <= now {
			byT[row.T] = row
		}
		return nil
	}

	// Open/recovery rows are loaded first. Sealed history is authoritative and
	// overwrites any duplicate target timestamp below.
	for _, raw := range openRows {
		if err := put(raw); err != nil {
			return err
		}
	}
	for _, c := range chunks {
		raws, err := readGzipRows(filepath.Join(datasetDir, filepath.FromSlash(c.Rel)))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		for _, raw := range raws {
			if err := put(raw); err != nil {
				return err
			}
		}
	}

	rows := make([]SpotSnapshot, 0, len(byT))
	for _, row := range byT {
		rows = append(rows, row)
	}
	b.Replace(rows)
	return nil
}
