package main

import (
	"math"
	"sort"
	"sync"
)

type Side int

const (
	Bid Side = iota
	Ask
)

type Level struct{ Price, Qty float64 }
type BookEvent struct {
	RecvMs     int64
	Role       string // near,broad,both
	Kind       string // replace,refresh,level,delta,status,touch
	Side       Side
	Price, Qty float64
	Bids, Asks []Level
	BidEdge    float64
	AskEdge    float64
	Connected  *bool
	Status     string
}

type Book struct {
	bids  map[float64]float64
	asks  map[float64]float64
	ready bool
}

func NewBook() *Book { return &Book{bids: map[float64]float64{}, asks: map[float64]float64{}} }
func (b *Book) Replace(bids, asks []Level) {
	b.bids = map[float64]float64{}
	b.asks = map[float64]float64{}
	for _, x := range bids {
		if x.Qty > 0 {
			b.bids[x.Price] = x.Qty
		}
	}
	for _, x := range asks {
		if x.Qty > 0 {
			b.asks[x.Price] = x.Qty
		}
	}
	b.ready = true
}

func (b *Book) RefreshRange(bids, asks []Level, bidEdge, askEdge float64) {
	// REST5000 proves the current book is complete only inside its absolute
	// snapshot interval. Replace that interval authoritatively while retaining
	// sequence-maintained observed levels outside it for the RAM/full-retention
	// experiment.
	if bidEdge > 0 {
		for p := range b.bids {
			if p >= bidEdge {
				delete(b.bids, p)
			}
		}
	}
	if askEdge > 0 {
		for p := range b.asks {
			if p <= askEdge {
				delete(b.asks, p)
			}
		}
	}
	for _, x := range bids {
		if x.Qty > 0 {
			b.bids[x.Price] = x.Qty
		}
	}
	for _, x := range asks {
		if x.Qty > 0 {
			b.asks[x.Price] = x.Qty
		}
	}
	b.ready = true
}
func (b *Book) Update(side Side, p, q float64) {
	m := b.bids
	if side == Ask {
		m = b.asks
	}
	if q <= 0 {
		delete(m, p)
	} else {
		m[p] = q
	}
}
func (b *Book) bestBid() float64 {
	x := -math.MaxFloat64
	for p := range b.bids {
		if p > x {
			x = p
		}
	}
	if x == -math.MaxFloat64 {
		return 0
	}
	return x
}
func (b *Book) bestAsk() float64 {
	x := math.MaxFloat64
	for p := range b.asks {
		if p < x {
			x = p
		}
	}
	if x == math.MaxFloat64 {
		return 0
	}
	return x
}
func (b *Book) Mid() float64 {
	bb, ba := b.bestBid(), b.bestAsk()
	if bb <= 0 || ba <= 0 {
		return 0
	}
	return (bb + ba) / 2
}
func (b *Book) Coverage(mid float64) (float64, float64) {
	if mid <= 0 {
		return 0, 0
	}
	minBid := math.MaxFloat64
	maxAsk := -math.MaxFloat64
	for p := range b.bids {
		if p < minBid {
			minBid = p
		}
	}
	for p := range b.asks {
		if p > maxAsk {
			maxAsk = p
		}
	}
	cb, ca := 0.0, 0.0
	if minBid != math.MaxFloat64 {
		cb = (mid - minBid) / mid * 10000
	}
	if maxAsk != -math.MaxFloat64 {
		ca = (maxAsk - mid) / mid * 10000
	}
	return cb, ca
}
func (b *Book) zoneWithCoverage(z Zone, bidCoverage, askCoverage float64) CompactPair {
	var out CompactPair
	if !b.ready {
		return out
	}
	mid := b.Mid()
	if mid <= 0 {
		return out
	}
	if bidCoverage+1e-9 >= z.High {
		bid := 0.0
		for p, q := range b.bids {
			d := (mid - p) / mid * 10000
			if d >= z.Low && d < z.High {
				bid += p * q
			}
		}
		out[0] = ptr(bid)
	}
	if askCoverage+1e-9 >= z.High {
		ask := 0.0
		for p, q := range b.asks {
			d := (p - mid) / mid * 10000
			if d >= z.Low && d < z.High {
				ask += p * q
			}
		}
		out[1] = ptr(ask)
	}
	return out
}

func (b *Book) Zone(z Zone) CompactPair {
	mid := b.Mid()
	if mid <= 0 {
		return CompactPair{}
	}
	cb, ca := b.Coverage(mid)
	return b.zoneWithCoverage(z, cb, ca)
}

type SourceState struct {
	mu                 sync.Mutex
	Venue, Asset, Pair string
	near, broad        *Book
	pending            []BookEvent
	nearConnected      bool
	broadConnected     bool
	nearStatus         string
	broadStatus        string
	lastNearMs         int64
	lastBroadMs        int64
	lastNearDataMs     int64
	lastBroadDataMs    int64
	gaps, resets       uint64
	cutoffMs            int64
	trustedBidEdge      float64
	trustedAskEdge      float64
}

func NewSource(v, a, p string) *SourceState {
	return &SourceState{
		Venue: v, Asset: a, Pair: p,
		near: NewBook(), broad: NewBook(),
		nearStatus: "STARTING", broadStatus: "STARTING",
	}
}
func (s *SourceState) Enqueue(e BookEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Normal operation keeps cutoffMs at the next decision boundary.
	// Events that are already causally eligible for that boundary can be
	// applied immediately. Only events arriving after the boundary during
	// the short finalize window need to sit in pending. This preserves the
	// receiveTs <= target rule while avoiding a 15-second event backlog.
	if s.cutoffMs > 0 && e.RecvMs <= s.cutoffMs {
		s.apply(e)
		return
	}
	s.pending = append(s.pending, e)
	if len(s.pending) > 1 && s.pending[len(s.pending)-2].RecvMs > e.RecvMs {
		sort.SliceStable(s.pending, func(i, j int) bool { return s.pending[i].RecvMs < s.pending[j].RecvMs })
	}
}
func (s *SourceState) Touch(recv int64) { s.TouchRole(recv, "both") }
func (s *SourceState) TouchRole(recv int64, role string) {
	s.Enqueue(BookEvent{RecvMs: recv, Role: role, Kind: "touch"})
}
func (s *SourceState) GapRole(recv int64, role, reason string) {
	f := false
	s.Enqueue(BookEvent{RecvMs: recv, Role: role, Kind: "status", Connected: &f, Status: reason})
	s.mu.Lock()
	s.gaps++
	s.mu.Unlock()
}
func (s *SourceState) ResetCount() { s.mu.Lock(); s.resets++; s.mu.Unlock() }
func (s *SourceState) markRole(role string, connected *bool, status string, recv int64) {
	if role == "" || role == "both" || role == "near" {
		if connected != nil {
			s.nearConnected = *connected
		}
		if status != "" {
			s.nearStatus = status
		}
		if recv > s.lastNearMs {
			s.lastNearMs = recv
		}
	}
	if role == "" || role == "both" || role == "broad" {
		if connected != nil {
			s.broadConnected = *connected
		}
		if status != "" {
			s.broadStatus = status
		}
		if recv > s.lastBroadMs {
			s.lastBroadMs = recv
		}
	}
}
func (s *SourceState) markDataRole(role string, recv int64) {
	if role == "" || role == "both" || role == "near" {
		if recv > s.lastNearDataMs {
			s.lastNearDataMs = recv
		}
	}
	if role == "" || role == "both" || role == "broad" {
		if recv > s.lastBroadDataMs {
			s.lastBroadDataMs = recv
		}
	}
}

func (s *SourceState) apply(e BookEvent) {
	switch e.Kind {
	case "replace":
		if e.Role == "near" || e.Role == "both" {
			s.near.Replace(e.Bids, e.Asks)
		}
		if e.Role == "broad" || e.Role == "both" {
			s.broad.Replace(e.Bids, e.Asks)
		}
		t := true
		s.markRole(e.Role, &t, "OK", e.RecvMs)
		s.markDataRole(e.Role, e.RecvMs)
	case "refresh":
		if e.Role == "near" || e.Role == "both" {
			s.near.RefreshRange(e.Bids, e.Asks, e.BidEdge, e.AskEdge)
		}
		if e.Role == "broad" || e.Role == "both" {
			s.broad.RefreshRange(e.Bids, e.Asks, e.BidEdge, e.AskEdge)
		}
		t := true
		s.markRole(e.Role, &t, "OK", e.RecvMs)
		s.markDataRole(e.Role, e.RecvMs)
	case "level":
		if e.Role == "near" || e.Role == "both" {
			s.near.Update(e.Side, e.Price, e.Qty)
		}
		if e.Role == "broad" || e.Role == "both" {
			s.broad.Update(e.Side, e.Price, e.Qty)
		}
		s.markRole(e.Role, nil, "", e.RecvMs)
		s.markDataRole(e.Role, e.RecvMs)
	case "delta":
		if e.Role == "near" || e.Role == "both" {
			for _, x := range e.Bids {
				s.near.Update(Bid, x.Price, x.Qty)
			}
			for _, x := range e.Asks {
				s.near.Update(Ask, x.Price, x.Qty)
			}
		}
		if e.Role == "broad" || e.Role == "both" {
			for _, x := range e.Bids {
				s.broad.Update(Bid, x.Price, x.Qty)
			}
			for _, x := range e.Asks {
				s.broad.Update(Ask, x.Price, x.Qty)
			}
		}
		s.markRole(e.Role, nil, "", e.RecvMs)
		s.markDataRole(e.Role, e.RecvMs)
	case "status":
		s.markRole(e.Role, e.Connected, e.Status, e.RecvMs)
	case "touch":
		s.markRole(e.Role, nil, "", e.RecvMs)
	}
}
func (s *SourceState) advanceLocked(target int64) {
	n := 0
	for n < len(s.pending) && s.pending[n].RecvMs <= target {
		s.apply(s.pending[n])
		n++
	}
	if n > 0 {
		copy(s.pending, s.pending[n:])
		s.pending = s.pending[:len(s.pending)-n]
	}
}
func (s *SourceState) SetCutoff(target int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cutoffMs = target
	s.advanceLocked(target)
}
func (s *SourceState) snapshotZonesLocked(target int64, maxAgeMs int64) []CompactPair {
	out := make([]CompactPair, len(Zones))
	nearLast, broadLast := s.lastNearMs, s.lastBroadMs
	// Binance local-book validity must be based on real depth data, not only
	// websocket ping frames. This prevents a connected-but-stale book from
	// being accepted as fresh.
	if s.Venue == "binance" {
		nearLast, broadLast = s.lastNearDataMs, s.lastBroadDataMs
	}
	nearOK := s.nearConnected && nearLast > 0 && target-nearLast <= maxAgeMs && s.near.ready
	broadOK := s.broadConnected && broadLast > 0 && target-broadLast <= maxAgeMs && s.broad.ready
	for i, z := range Zones {
		var book *Book
		if z.High <= 300 {
			if nearOK {
				book = s.near
			} else if broadOK {
				book = s.broad
			}
		} else if broadOK {
			book = s.broad
		}
		if book == nil {
			continue
		}
		// Production collection uses the causally maintained local book for
		// zone values. For Binance BTC/ETH that book starts from REST5000 and is
		// continuously extended/updated by sequence-valid diff-depth events.
		// REST bootstrap edges remain available in CoverageAudit as a separate
		// quality diagnostic; they no longer suppress observed 0-750 bps values.
		out[i] = book.Zone(z)
	}
	return out
}
func (s *SourceState) SnapshotZones(target int64, maxAgeMs int64) []CompactPair {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advanceLocked(target)
	s.cutoffMs = target
	return s.snapshotZonesLocked(target, maxAgeMs)
}
func (s *SourceState) SnapshotZonesAdvance(target, nextTarget, maxAgeMs int64) []CompactPair {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advanceLocked(target)
	out := s.snapshotZonesLocked(target, maxAgeMs)
	s.cutoffMs = nextTarget
	// At finalize time only the tiny (target, finalize] tail should be here.
	// Applying it now prepares the working book for the next boundary.
	s.advanceLocked(nextTarget)
	return out
}

type CoverageAudit struct {
	Ready             bool    `json:"ready"`
	Connected         bool    `json:"connected"`
	AgeMs             int64   `json:"ageMs"`
	DepthAgeMs        int64   `json:"depthAgeMs"`
	ObservedBid       float64 `json:"observedBidBps"`
	ObservedAsk       float64 `json:"observedAskBps"`
	TrustedBid        float64 `json:"trustedBidBps"`
	TrustedAsk        float64 `json:"trustedAskBps"`
	ValidBidLevels    int     `json:"validBidLevels"`
	ValidAskLevels    int     `json:"validAskLevels"`
	TargetBps         float64 `json:"targetBps"`
	TargetBidReached  bool    `json:"targetBidReached"`
	TargetAskReached  bool    `json:"targetAskReached"`
	TargetBothReached bool    `json:"targetBothReached"`
}

func (s *SourceState) SetTrustedEdges(bidEdge, askEdge float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trustedBidEdge = bidEdge
	s.trustedAskEdge = askEdge
}

func (s *SourceState) trustedCoverageLocked(mid float64) (float64, float64) {
	if mid <= 0 {
		return 0, 0
	}
	tb, ta := 0.0, 0.0
	if s.trustedBidEdge > 0 && s.trustedBidEdge < mid {
		tb = (mid - s.trustedBidEdge) / mid * 10000
	}
	if s.trustedAskEdge > mid {
		ta = (s.trustedAskEdge - mid) / mid * 10000
	}
	return tb, ta
}


func (s *SourceState) TrustedCoverageNow() (bool, float64, float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broad == nil || !s.broad.ready {
		return false, 0, 0
	}
	mid := s.broad.Mid()
	if mid <= 0 {
		return false, 0, 0
	}
	tb, ta := s.trustedCoverageLocked(mid)
	return true, tb, ta
}

func (s *SourceState) CoverageAudit(target int64) CoverageAudit {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advanceLocked(target)
	b := s.broad
	if b == nil || !b.ready {
		return CoverageAudit{}
	}
	mid := b.Mid()
	if mid <= 0 {
		return CoverageAudit{}
	}
	ob, oa := b.Coverage(mid)
	tb, ta := s.trustedCoverageLocked(mid)
	age := int64(0)
	if s.lastBroadMs > 0 {
		age = target - s.lastBroadMs
		if age < 0 {
			age = 0
		}
	}
	depthAge := int64(0)
	if s.lastBroadDataMs > 0 {
		depthAge = target - s.lastBroadDataMs
		if depthAge < 0 {
			depthAge = 0
		}
	}
	targetBps := 0.0
	if len(Zones) > 0 {
		targetBps = Zones[len(Zones)-1].High
	}
	// Target reach follows the same observed local-book coverage used by
	// production zone emission. Trusted REST coverage is still reported
	// separately in TrustedBid / TrustedAsk for audit and research filtering.
	bidReached, askReached := ob+1e-9 >= targetBps, oa+1e-9 >= targetBps
	return CoverageAudit{
		Ready: true, Connected: s.broadConnected, AgeMs: age, DepthAgeMs: depthAge,
		ObservedBid: ob, ObservedAsk: oa, TrustedBid: tb, TrustedAsk: ta,
		ValidBidLevels: len(b.bids), ValidAskLevels: len(b.asks), TargetBps: targetBps,
		TargetBidReached: bidReached, TargetAskReached: askReached, TargetBothReached: bidReached && askReached,
	}
}

func (s *SourceState) Counters() (uint64, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gaps, s.resets
}
