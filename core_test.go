package main

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func readySource() *SourceState {
	s := NewSource("x", "BTC", "X")
	bids := []Level{{99.99, 1}, {99.8, 2}, {99.0, 3}, {95, 4}, {85, 5}, {79, 6}}
	asks := []Level{{100.01, 1}, {100.2, 2}, {101, 3}, {105, 4}, {115, 5}, {121, 6}}
	s.Enqueue(BookEvent{RecvMs: 1000, Role: "both", Kind: "replace", Bids: bids, Asks: asks})
	s.Touch(1000)
	return s
}

func TestFiveZones(t *testing.T) {
	if len(Zones) != 5 {
		t.Fatal(len(Zones))
	}
	want := []Zone{{0, 100}, {100, 200}, {200, 300}, {300, 500}, {500, 750}}
	for i := range want {
		if Zones[i] != want[i] {
			t.Fatalf("zone %d", i)
		}
	}
}
func TestMissingIsNullWhenCoverageIncomplete(t *testing.T) {
	s := readySource()
	r := s.SnapshotZones(1000, 45000)
	if r[0][0] == nil || r[0][1] == nil {
		t.Fatal("near zone missing")
	}
	if r[4][0] == nil || r[4][1] == nil {
		t.Fatal("expected 750bps coverage")
	}
	q := NewSource("x", "BTC", "X")
	q.Enqueue(BookEvent{RecvMs: 1000, Role: "both", Kind: "replace", Bids: []Level{{99, 1}}, Asks: []Level{{101, 1}}})
	out := q.SnapshotZones(1000, 45000)
	if out[4][0] != nil || out[4][1] != nil {
		t.Fatal("incomplete coverage must be null")
	}
}
func TestCausalCutoff(t *testing.T) {
	s := readySource()
	s.Enqueue(BookEvent{RecvMs: 1100, Role: "both", Kind: "level", Side: Bid, Price: 99.99, Qty: 100})
	a := s.SnapshotZones(1000, 45000)
	b := s.SnapshotZones(1100, 45000)
	if a[0][0] == nil || b[0][0] == nil {
		t.Fatal("missing")
	}
	if *a[0][0] >= *b[0][0] {
		t.Fatalf("future update leaked or not applied: a=%v b=%v", *a[0][0], *b[0][0])
	}
}
func TestCompactSnapshotShape(t *testing.T) {
	d := make([][][]CompactPair, 4)
	for i := range d {
		d[i] = make([][]CompactPair, 3)
		for j := range d[i] {
			d[i][j] = make([]CompactPair, 5)
		}
	}
	r := SpotSnapshot{T: 1, A: 2, D: d}
	if len(r.D) != 4 || len(r.D[0]) != 3 || len(r.D[0][0]) != 5 {
		t.Fatal("shape")
	}
}
func TestStoreHistoryChunk(t *testing.T) {
	dir := t.TempDir()
	cfg := LoadConfig()
	cfg.DatasetDir = filepath.Join(dir, "ds")
	cfg.ChunkDuration = time.Second
	cfg.PackageDuration = 12 * time.Hour
	s, err := NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Add("spot", 1000, SpotSnapshot{T: 1000, A: 1001}); err != nil {
		t.Fatal(err)
	}
	ms, err := s.FlushClosed(2500)
	if err != nil || len(ms) != 1 {
		t.Fatalf("flush %v %v", len(ms), err)
	}
	xs, next := s.History(0, 0, 100, 0)
	if len(xs) != 1 || xs[0].Rows != 1 || next != -1 {
		t.Fatalf("history %#v", xs)
	}
	if _, err = os.Stat(filepath.Join(cfg.DatasetDir, filepath.FromSlash(xs[0].Rel))); err != nil {
		t.Fatal(err)
	}
}


func TestOneSidedCoverageKeepsValidSide(t *testing.T) {
	q := NewSource("x", "BTC", "X")
	q.Enqueue(BookEvent{RecvMs: 1000, Role: "both", Kind: "replace",
		Bids: []Level{{99.99, 1}, {92, 2}},
		Asks: []Level{{100.01, 1}, {105, 2}},
	})
	out := q.SnapshotZones(1000, 45000)
	if out[4][0] == nil {
		t.Fatal("valid bid coverage must be preserved")
	}
	if out[4][1] != nil {
		t.Fatal("incomplete ask coverage must be null")
	}
}


func TestRecoveryCheckpointSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := LoadConfig()
	cfg.DatasetDir = filepath.Join(dir, "ds")
	cfg.ChunkDuration = 10 * time.Second
	cfg.PackageDuration = 12 * time.Hour

	s, err := NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Add("spot", 1000, SpotSnapshot{T: 1000, A: 1001}); err != nil {
		t.Fatal(err)
	}
	if err = s.CheckpointOpen(); err != nil {
		t.Fatal(err)
	}

	s2, err := NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = s2.Add("spot", 2000, SpotSnapshot{T: 2000, A: 2001}); err != nil {
		t.Fatal(err)
	}
	ms, err := s2.FlushClosed(10001)
	if err != nil {
		t.Fatal(err)
	}
	var spot ChunkMeta
	found := false
	for _, m := range ms {
		if m.Kind == "spot" {
			spot = m
			found = true
		}
	}
	if !found || spot.Rows != 2 {
		t.Fatalf("recovered spot rows=%d found=%v", spot.Rows, found)
	}
	if _, err := os.Stat(s2.recoveryPath("spot", 0)); !os.IsNotExist(err) {
		t.Fatalf("recovery checkpoint should be removed after final seal: %v", err)
	}
}

func TestPackageMaxBytesActuallySeals(t *testing.T) {
	dir := t.TempDir()
	cfg := LoadConfig()
	cfg.DatasetDir = filepath.Join(dir, "ds")
	cfg.ChunkDuration = time.Second
	cfg.PackageDuration = 12 * time.Hour
	cfg.PackageMaxBytes = 1
	s, err := NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Add("spot", 1000, SpotSnapshot{T: 1000, A: 1001}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.FlushClosed(2500); err != nil {
		t.Fatal(err)
	}
	if len(s.Packages()) != 1 {
		t.Fatalf("expected size-triggered package seal, got %d", len(s.Packages()))
	}
}


func TestSpotRESTAcceptWindow(t *testing.T) {
	target := int64(10000)
	if !withinSpotWindow(target, target-1500) || !withinSpotWindow(target, target+1500) {
		t.Fatal("window edges must be accepted")
	}
	if withinSpotWindow(target, target-1501) || withinSpotWindow(target, target+1501) {
		t.Fatal("outside ±1.5s must be rejected")
	}
}

func TestRESTZonesMatchBookZoneMath(t *testing.T) {
	bids := []Level{{99.99, 1}, {99.8, 2}, {99.0, 3}, {95, 4}, {85, 5}, {79, 6}}
	asks := []Level{{100.01, 1}, {100.2, 2}, {101, 3}, {105, 4}, {115, 5}, {121, 6}}
	got := zonesFromLevels(bids, asks)
	s := NewSource("x", "BTC", "X")
	s.Enqueue(BookEvent{RecvMs: 1000, Role: "both", Kind: "replace", Bids: bids, Asks: asks})
	want := s.SnapshotZones(1000, 45000)
	for i := range Zones {
		for side := 0; side < 2; side++ {
			if (got[i][side] == nil) != (want[i][side] == nil) {
				t.Fatalf("zone=%d side=%d nil mismatch", i, side)
			}
			if got[i][side] != nil && math.Abs(*got[i][side]-*want[i][side]) > 1e-12 {
				t.Fatalf("zone=%d side=%d got=%v want=%v", i, side, *got[i][side], *want[i][side])
			}
		}
	}
}


func TestBitfinexMergeUsesBroadFrom100Bps(t *testing.T) {
	mk := func(v float64) *float64 { return &v }
	near := make([]CompactPair, len(Zones))
	broad := make([]CompactPair, len(Zones))
	for i := range Zones {
		near[i] = CompactPair{mk(float64(10 + i)), mk(float64(20 + i))}
		broad[i] = CompactPair{mk(float64(100 + i)), mk(float64(200 + i))}
	}
	got := mergeBitfinexZones(near, broad)
	for i := range Zones {
		wantBid := float64(100 + i)
		if Zones[i].High <= 100 {
			wantBid = float64(10 + i)
		}
		if got[i][0] == nil || *got[i][0] != wantBid {
			t.Fatalf("zone=%d high=%v got=%v want=%v", i, Zones[i].High, got[i][0], wantBid)
		}
	}
}


func TestBinanceCoverageSeparatesObservedFromBootstrapTrusted(t *testing.T) {
	s := NewSource("binance", "BTC", "BTCUSDT")
	s.SetCutoff(10_000)
	// Initial bootstrap only proves the absolute range 90..110 around mid 100.
	s.Enqueue(BookEvent{RecvMs: 1000, Role: "both", Kind: "replace",
		Bids: []Level{{99, 1}, {90, 1}},
		Asks: []Level{{101, 1}, {110, 1}},
	})
	s.SetTrustedEdges(90, 110)
	// Later diff-depth updates may reveal farther prices, but those do not prove
	// that every untouched price level between the old snapshot edge and here was known.
	s.Enqueue(BookEvent{RecvMs: 2000, Role: "both", Kind: "level", Side: Bid, Price: 70, Qty: 1})
	s.Enqueue(BookEvent{RecvMs: 2000, Role: "both", Kind: "level", Side: Ask, Price: 130, Qty: 1})
	a := s.CoverageAudit(2000)
	if a.ObservedBid <= a.TrustedBid || a.ObservedAsk <= a.TrustedAsk {
		t.Fatalf("expected observed coverage to exceed trusted bootstrap coverage: %#v", a)
	}
	if math.Abs(a.TrustedBid-1000) > 1e-9 || math.Abs(a.TrustedAsk-1000) > 1e-9 {
		t.Fatalf("unexpected trusted coverage: %#v", a)
	}
}

func TestBinanceSnapshotEdges(t *testing.T) {
	bids := map[float64]float64{100: 1, 95: 2, 90: 3}
	asks := map[float64]float64{101: 1, 110: 2, 120: 3}
	b, a := binanceTrustedEdges(bids, asks)
	if b != 90 || a != 120 {
		t.Fatalf("got bid=%v ask=%v", b, a)
	}
}


func TestBinanceDiffJSONDecodesUpdateIDs(t *testing.T) {
	var m binDepth
	if err := json.Unmarshal([]byte(`{"U":157,"u":160,"b":[["100","1"]],"a":[["101","2"]]}`), &m); err != nil {
		t.Fatal(err)
	}
	if m.FirstUpdateID != 157 || m.LastUpdateID != 160 {
		t.Fatalf("unexpected update ids: first=%d last=%d", m.FirstUpdateID, m.LastUpdateID)
	}
	if len(m.B) != 1 || len(m.A) != 1 {
		t.Fatalf("unexpected depth payload: %#v", m)
	}
}

func TestBinanceBroadOnlyBookServesAllZones(t *testing.T) {
	s := NewSource("binance", "BTC", "BTCUSDT")
	s.SetCutoff(10_000)
	s.Enqueue(BookEvent{RecvMs: 1000, Role: "broad", Kind: "replace",
		Bids: []Level{{99.99, 1}, {99.8, 2}, {99.0, 3}, {95, 4}, {85, 5}, {79, 6}},
		Asks: []Level{{100.01, 1}, {100.2, 2}, {101, 3}, {105, 4}, {115, 5}, {121, 6}},
	})
	out := s.SnapshotZones(1000, 45_000)
	for i := range Zones {
		if out[i][0] == nil || out[i][1] == nil {
			t.Fatalf("zone %d should be served by broad-only Binance book", i)
		}
	}
	a := s.CoverageAudit(1000)
	if a.DepthAgeMs != 0 || a.ValidBidLevels != 6 || a.ValidAskLevels != 6 {
		t.Fatalf("unexpected Binance audit: %#v", a)
	}
}


func TestMergePreferExactOnlyFillsNullSides(t *testing.T) {
	mk := func(v float64) *float64 { return &v }
	exact := make([]CompactPair, len(Zones))
	grouped := make([]CompactPair, len(Zones))
	exact[0] = CompactPair{mk(1), mk(2)}
	exact[1] = CompactPair{mk(3), nil}
	grouped[0] = CompactPair{mk(100), mk(200)}
	grouped[1] = CompactPair{mk(300), mk(400)}
	got := mergePreferExact(exact, grouped)
	if got[0][0] == nil || *got[0][0] != 1 || got[0][1] == nil || *got[0][1] != 2 {
		t.Fatalf("exact zone must win: %#v", got[0])
	}
	if got[1][0] == nil || *got[1][0] != 3 || got[1][1] == nil || *got[1][1] != 400 {
		t.Fatalf("grouped must only fill null side: %#v", got[1])
	}
}



func TestThirtySecondSpotCadence(t *testing.T) {
	cfg := LoadConfig()
	if cfg.SpotCadence != 30*time.Second {
		t.Fatalf("spot cadence=%v want=30s", cfg.SpotCadence)
	}
	if cfg.ChunkDuration != 15*time.Minute {
		t.Fatalf("chunk duration=%v want=15m", cfg.ChunkDuration)
	}
}

func TestConfirmedEmptyZoneIsZeroNotNull(t *testing.T) {
	bids := []Level{{99.99, 1}, {92, 1}}
	asks := []Level{{100.01, 1}, {108, 1}}
	out := zonesFromLevels(bids, asks)
	for i := range Zones {
		if out[i][0] == nil || out[i][1] == nil {
			t.Fatalf("zone %d must be covered, got %#v", i, out[i])
		}
	}
	if *out[1][0] != 0 || *out[1][1] != 0 {
		t.Fatalf("covered empty zone must be zero: %#v", out[1])
	}
}

func TestOuterCoverageLossDoesNotBlankInnerZones(t *testing.T) {
	bids := []Level{
		{99.99, 1}, {99.2, 1}, {98.5, 1}, {97.5, 1}, {96, 1}, {92, 1},
	}
	asks := []Level{
		{100.01, 1}, {100.8, 1}, {101.5, 1}, {102.5, 1}, {104, 1}, {106, 1},
	}
	out := zonesFromLevels(bids, asks)
	for i := 0; i < 4; i++ {
		if out[i][0] == nil || out[i][1] == nil {
			t.Fatalf("inner zone %d must remain valid: %#v", i, out[i])
		}
	}
	if out[4][0] == nil {
		t.Fatal("500-750 bid should be covered")
	}
	if out[4][1] != nil {
		t.Fatal("500-750 ask should be null because ask coverage stops before 750 bps")
	}
}
