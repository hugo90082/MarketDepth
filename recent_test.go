package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRecentBufferKeepsSixteenMinutes(t *testing.T) {
	b := NewRecentBuffer(16 * time.Minute)
	for i := 0; i < 33; i++ {
		b.Add(SpotSnapshot{T: int64(i) * 30_000})
	}
	rows, oldest, latest := b.RowsSince(0)
	if len(rows) != 32 {
		t.Fatalf("recent rows=%d want=32", len(rows))
	}
	if oldest != 30_000 || latest != 960_000 {
		t.Fatalf("oldest=%d latest=%d", oldest, latest)
	}
	inc, _, _ := b.RowsSince(latest - 60_000)
	if len(inc) != 2 || inc[0].T != latest-30_000 || inc[1].T != latest {
		t.Fatalf("incremental rows=%#v", inc)
	}
}

func TestCooldownLimiterThirtySeconds(t *testing.T) {
	l := NewCooldownLimiter(30 * time.Second)
	if ok, _ := l.AllowAt(1_000); !ok {
		t.Fatal("first request must pass")
	}
	if ok, retry := l.AllowAt(30_999); ok || retry != 1 {
		t.Fatalf("request before 30s must fail: ok=%v retry=%d", ok, retry)
	}
	if ok, retry := l.AllowAt(31_000); !ok || retry != 0 {
		t.Fatalf("request at 30s must pass: ok=%v retry=%d", ok, retry)
	}
}

func TestRecentBufferRebuildsFromSealedAndOpenStore(t *testing.T) {
	dir := t.TempDir()
	cfg := LoadConfig()
	cfg.DatasetDir = filepath.Join(dir, "ds")
	cfg.ChunkDuration = time.Minute
	cfg.PackageDuration = 12 * time.Hour

	s, err := NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Minute).Add(30 * time.Second).UnixMilli()
	sealedT := now - int64((90*time.Second)/time.Millisecond)
	openT := now - int64((30*time.Second)/time.Millisecond)
	if err := s.Add("spot", sealedT, SpotSnapshot{T: sealedT, A: sealedT + 100}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FlushClosed(now); err != nil {
		t.Fatal(err)
	}
	if err := s.Add("spot", openT, SpotSnapshot{T: openT, A: openT + 100}); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckpointOpen(); err != nil {
		t.Fatal(err)
	}

	b := NewRecentBuffer(16 * time.Minute)
	if err := b.RebuildFromStore(s, now); err != nil {
		t.Fatal(err)
	}
	rows, oldest, latest := b.RowsSince(0)
	if len(rows) != 2 {
		t.Fatalf("rebuilt rows=%d want=2", len(rows))
	}
	if oldest != sealedT || latest != openT {
		t.Fatalf("oldest=%d latest=%d want=%d,%d", oldest, latest, sealedT, openT)
	}
}
