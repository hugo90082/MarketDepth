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
	xs, next, version, consistent := s.History(0, 0, 100, 0, 0)
	if len(xs) != 1 || xs[0].Rows != 1 || next != -1 || version <= 0 || !consistent {
		t.Fatalf("history %#v next=%d version=%d consistent=%v", xs, next, version, consistent)
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

func TestDepthValuesAreUSDNotional(t *testing.T) {
	bids := []Level{{99.5, 2}, {90, 1}}
	asks := []Level{{100.5, 2}, {110, 1}}

	rest := zonesFromLevels(bids, asks)
	if rest[0][0] == nil || rest[0][1] == nil {
		t.Fatalf("near zone missing: %#v", rest[0])
	}
	if math.Abs(*rest[0][0]-199.0) > 1e-9 || math.Abs(*rest[0][1]-201.0) > 1e-9 {
		t.Fatalf("REST depth must be price*qty USD notional, got bid=%v ask=%v", *rest[0][0], *rest[0][1])
	}

	s := NewSource("x", "BTC", "BTCUSDT")
	s.Enqueue(BookEvent{RecvMs: 1000, Role: "both", Kind: "replace", Bids: bids, Asks: asks})
	book := s.SnapshotZones(1000, 45000)
	if book[0][0] == nil || book[0][1] == nil {
		t.Fatalf("book near zone missing: %#v", book[0])
	}
	if math.Abs(*book[0][0]-199.0) > 1e-9 || math.Abs(*book[0][1]-201.0) > 1e-9 {
		t.Fatalf("local-book depth must be price*qty USD notional, got bid=%v ask=%v", *book[0][0], *book[0][1])
	}
}

func TestLegacyDatasetIsResetOnceForUSDFormat(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ds")
	if err := os.MkdirAll(filepath.Join(dir, "history"), 0755); err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(dir, "history", "legacy.txt")
	if err := os.WriteFile(legacy, []byte("base-qty"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := LoadConfig()
	cfg.DatasetDir = dir
	cfg.ChunkDuration = time.Second
	cfg.PackageDuration = 12 * time.Hour
	if _, err := NewStore(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatalf("legacy dataset must be deleted on format migration: %v", err)
	}
	marker := filepath.Join(dir, ".marketdepth-format")
	b, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != DatasetFormat+"\n" {
		t.Fatalf("unexpected dataset marker %q", string(b))
	}

	keep := filepath.Join(dir, "meta", "keep.txt")
	if err := os.WriteFile(keep, []byte("new-format"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("matching USD dataset must survive restart: %v", err)
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


func TestBitfinexMergePrefersP0ThenP1NearAndP2Broad(t *testing.T) {
	mk := func(v float64) *float64 { return &v }
	p0 := make([]CompactPair, len(Zones))
	p1 := make([]CompactPair, len(Zones))
	p2 := make([]CompactPair, len(Zones))

	// P0 has the preferred bid but cannot prove the ask reaches 100 bps.
	p0[0] = CompactPair{mk(10), nil}
	// P1 can prove both sides. It must fill only the missing ask, not
	// overwrite the higher-resolution P0 bid.
	p1[0] = CompactPair{mk(20), mk(21)}
	for i := range Zones {
		p2[i] = CompactPair{mk(float64(100 + i)), mk(float64(200 + i))}
	}

	got := mergeBitfinexZones(p0, p1, p2)
	if got[0][0] == nil || *got[0][0] != 10 {
		t.Fatalf("0-100 bid must prefer P0, got=%v", got[0][0])
	}
	if got[0][1] == nil || *got[0][1] != 21 {
		t.Fatalf("0-100 ask must fall back to P1, got=%v", got[0][1])
	}
	for i := 1; i < len(Zones); i++ {
		if got[i][0] == nil || *got[i][0] != float64(100+i) {
			t.Fatalf("zone=%d bid must remain P2, got=%v", i, got[i][0])
		}
		if got[i][1] == nil || *got[i][1] != float64(200+i) {
			t.Fatalf("zone=%d ask must remain P2, got=%v", i, got[i][1])
		}
	}
}

func TestBitfinexNearDoesNotFallBackToP2(t *testing.T) {
	mk := func(v float64) *float64 { return &v }
	p2 := make([]CompactPair, len(Zones))
	for i := range Zones {
		p2[i] = CompactPair{mk(float64(100 + i)), mk(float64(200 + i))}
	}
	got := mergeBitfinexZones(nil, nil, p2)
	if got[0][0] != nil || got[0][1] != nil {
		t.Fatalf("0-100 must stay null when both P0 and P1 lack coverage: %#v", got[0])
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

func TestBinanceOuterDiffCannotExpandTrustedZoneValidity(t *testing.T) {
	s := NewSource("binance", "BTC", "BTCUSDT")
	s.SetCutoff(10_000)

	// Bootstrap proves only about 150 bps on each side around mid=100.
	s.Enqueue(BookEvent{RecvMs: 1000, Role: "broad", Kind: "replace",
		Bids: []Level{{99.99, 1}, {99.5, 1}, {98.5, 1}},
		Asks: []Level{{100.01, 1}, {100.5, 1}, {101.5, 1}},
	})
	s.SetTrustedEdges(98.5, 101.5)

	// Later isolated diff levels appear much farther away. These expand observed
	// coverage only; they do not prove the untouched interval is complete.
	s.Enqueue(BookEvent{RecvMs: 2000, Role: "broad", Kind: "level", Side: Bid, Price: 90, Qty: 2})
	s.Enqueue(BookEvent{RecvMs: 2000, Role: "broad", Kind: "level", Side: Ask, Price: 110, Qty: 2})

	out := s.SnapshotZones(2000, 45_000)
	if out[0][0] == nil || out[0][1] == nil {
		t.Fatalf("trusted 0-100 bps zone must remain valid: %#v", out[0])
	}
	for i := 1; i < len(Zones); i++ {
		if out[i][0] != nil || out[i][1] != nil {
			t.Fatalf("outer diff must not authorize zone %d beyond trusted bootstrap coverage: %#v", i, out[i])
		}
	}

	a := s.CoverageAudit(2000)
	if a.ObservedBid <= a.TrustedBid || a.ObservedAsk <= a.TrustedAsk {
		t.Fatalf("expected observed coverage to exceed trusted coverage: %#v", a)
	}
	if a.TargetBidReached || a.TargetAskReached || a.TargetBothReached {
		t.Fatalf("target coverage flags must use trusted coverage, not observed: %#v", a)
	}
}

func TestBinanceDeltaLevelsRetainOuterPrices(t *testing.T) {
	e := timedBin{recv: 2000, msg: binDepth{
		B: [][]string{{"100", "2"}, {"90", "0"}, {"80", "7"}},
		A: [][]string{{"101", "3"}, {"110", "0"}, {"120", "8"}},
	}}
	bids, asks := binanceDeltaLevels(e)

	if len(bids) != 3 || len(asks) != 3 {
		t.Fatalf("full-retention delta lost levels: bids=%#v asks=%#v", bids, asks)
	}
	foundOuterBid, foundOuterAsk := false, false
	foundBidDelete, foundAskDelete := false, false
	for _, x := range bids {
		if x.Price == 80 && x.Qty == 7 {
			foundOuterBid = true
		}
		if x.Price == 90 && x.Qty == 0 {
			foundBidDelete = true
		}
	}
	for _, x := range asks {
		if x.Price == 120 && x.Qty == 8 {
			foundOuterAsk = true
		}
		if x.Price == 110 && x.Qty == 0 {
			foundAskDelete = true
		}
	}
	if !foundOuterBid || !foundOuterAsk || !foundBidDelete || !foundAskDelete {
		t.Fatalf("expected outer observations and zero-qty deletes to survive: bids=%#v asks=%#v", bids, asks)
	}
}

func TestOuterBinanceDeltaGrowsObservedBookButNotTrusted(t *testing.T) {
	s := NewSource("binance", "BTC", "BTCUSDT")
	s.SetCutoff(10_000)
	s.SetTrustedEdges(98.5, 101.5)
	s.Enqueue(BookEvent{RecvMs: 1000, Role: "broad", Kind: "replace",
		Bids: []Level{{100, 1}, {99, 1}, {98.5, 1}},
		Asks: []Level{{101, 1}, {101.2, 1}, {101.5, 1}},
	})
	before := s.CoverageAudit(1000)

	e := timedBin{recv: 2000, msg: binDepth{
		B: [][]string{{"80", "9"}},
		A: [][]string{{"120", "9"}},
	}}
	enqueueBinDelta(s, e)
	after := s.CoverageAudit(2000)

	if after.ValidBidLevels != before.ValidBidLevels+1 || after.ValidAskLevels != before.ValidAskLevels+1 {
		t.Fatalf("outer delta must grow observed book in full-retention mode: before=%#v after=%#v", before, after)
	}
	if after.ObservedBid <= after.TrustedBid || after.ObservedAsk <= after.TrustedAsk {
		t.Fatalf("observed coverage should exceed trusted coverage after outer levels: %#v", after)
	}
	if after.TargetBidReached || after.TargetAskReached || after.TargetBothReached {
		t.Fatalf("outer observed levels must not authorize trusted target coverage: %#v", after)
	}
	if after.DepthAgeMs != 0 {
		t.Fatalf("sequence-valid delta must refresh depth age: %#v", after)
	}
}

func TestBinanceRefreshRangeRetainsOuterObservedLevels(t *testing.T) {
	s := NewSource("binance", "BTC", "BTCUSDT")
	s.SetCutoff(10_000)
	s.SetTrustedEdges(90, 110)
	s.Enqueue(BookEvent{RecvMs: 1000, Role: "broad", Kind: "replace",
		Bids: []Level{{100, 1}, {95, 1}, {90, 1}, {80, 4}},
		Asks: []Level{{101, 1}, {105, 1}, {110, 1}, {120, 4}},
	})

	// A new REST snapshot proves only the new absolute interval. Refresh that
	// interval, but retain sequence-maintained observations outside it.
	s.SetTrustedEdges(92, 108)
	s.Enqueue(BookEvent{RecvMs: 2000, Role: "broad", Kind: "refresh",
		BidEdge: 92, AskEdge: 108,
		Bids: []Level{{100, 2}, {96, 2}, {92, 2}},
		Asks: []Level{{101, 2}, {104, 2}, {108, 2}},
	})

	if _, ok := s.broad.bids[80]; !ok {
		t.Fatal("outer observed bid should survive trusted-range refresh")
	}
	if _, ok := s.broad.asks[120]; !ok {
		t.Fatal("outer observed ask should survive trusted-range refresh")
	}
	if _, ok := s.broad.bids[95]; ok {
		t.Fatal("stale bid inside refreshed REST interval must be removed")
	}
	if _, ok := s.broad.asks[105]; ok {
		t.Fatal("stale ask inside refreshed REST interval must be removed")
	}
	if s.broad.bids[96] != 2 || s.broad.asks[104] != 2 {
		t.Fatalf("fresh REST levels missing after range refresh: bids=%#v asks=%#v", s.broad.bids, s.broad.asks)
	}
	a := s.CoverageAudit(2000)
	if a.ObservedBid <= a.TrustedBid || a.ObservedAsk <= a.TrustedAsk {
		t.Fatalf("retained outer levels should remain observed-only: %#v", a)
	}
}

func TestBinanceRefreshFloorIsAdaptive(t *testing.T) {
	cases := []struct {
		initial float64
		want    float64
	}{
		{100, 25},
		{40, 10},
		{8, 2},
		{2, 1},
		{0.5, 0.25},
	}
	for _, tc := range cases {
		got := binanceRefreshFloor(tc.initial)
		if math.Abs(got-tc.want) > 1e-9 {
			t.Fatalf("initial=%v got=%v want=%v", tc.initial, got, tc.want)
		}
	}
}

func TestTrustedCoverageNowDetectsBookSideDepletion(t *testing.T) {
	s := NewSource("binance", "BTC", "BTCUSDT")
	s.SetCutoff(10_000)
	s.Enqueue(BookEvent{RecvMs: 1000, Role: "broad", Kind: "replace",
		Bids: []Level{{100, 1}, {95, 1}, {90, 1}},
		Asks: []Level{{101, 1}, {105, 1}, {110, 1}},
	})
	s.SetTrustedEdges(90, 110)

	ready, bidBps, askBps := s.TrustedCoverageNow()
	if !ready || bidBps <= 0 || askBps <= 0 {
		t.Fatalf("fresh trusted book must be ready: ready=%v bid=%v ask=%v", ready, bidBps, askBps)
	}

	// Simulate price walking out of the fixed trusted window until every bid
	// inside it has been removed. This was the production failure mode.
	for _, p := range []float64{100, 95, 90} {
		s.Enqueue(BookEvent{RecvMs: 2000, Role: "broad", Kind: "level", Side: Bid, Price: p, Qty: 0})
	}
	ready, _, _ = s.TrustedCoverageNow()
	if ready {
		t.Fatal("empty trusted bid side must request rebootstrap")
	}
}

func TestBinanceMapMid(t *testing.T) {
	bids := map[float64]float64{100: 1, 99: 2}
	asks := map[float64]float64{101: 1, 102: 2}
	got := binanceMapMid(bids, asks)
	if math.Abs(got-100.5) > 1e-9 {
		t.Fatalf("mid=%v want=100.5", got)
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
	s.SetTrustedEdges(79, 121)
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


func TestHistoryIndexVersionRejectsShift(t *testing.T) {
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
	if _, err = s.FlushClosed(2500); err != nil {
		t.Fatal(err)
	}
	_, _, v1, ok := s.History(0, 0, 1, 0, 0)
	if !ok || v1 <= 0 {
		t.Fatalf("initial history version=%d ok=%v", v1, ok)
	}

	if err = s.Add("spot", 3000, SpotSnapshot{T: 3000, A: 3001}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.FlushClosed(4500); err != nil {
		t.Fatal(err)
	}
	rows, next, v2, ok := s.History(0, 0, 1, 1, v1)
	if ok {
		t.Fatalf("stale index version must be rejected rows=%#v next=%d old=%d new=%d", rows, next, v1, v2)
	}
	if v2 == v1 {
		t.Fatalf("index version must change after index mutation: %d", v2)
	}
}
