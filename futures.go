package main

import "sync"

type TradeEvent struct {
	RecvMs, ExchangeMs int64
	Price              float64
}
type FutureState struct {
	mu       sync.Mutex
	pending  []TradeEvent
	last     *TradeEvent
	cutoffMs int64
}

func (s *FutureState) apply(e TradeEvent) {
	x := e
	s.last = &x
}
func (s *FutureState) advanceLocked(target int64) {
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
func (s *FutureState) Add(e TradeEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cutoffMs > 0 && e.RecvMs <= s.cutoffMs {
		s.apply(e)
		return
	}
	s.pending = append(s.pending, e)
}
func (s *FutureState) SetCutoff(target int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cutoffMs = target
	s.advanceLocked(target)
}
func (s *FutureState) At(target int64) FuturesAsset {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advanceLocked(target)
	s.cutoffMs = target
	if s.last == nil {
		return FuturesAsset{}
	}
	return FuturesAsset{P: ptr(s.last.Price), X: s.last.ExchangeMs}
}
func (s *FutureState) AtAdvance(target, nextTarget int64) FuturesAsset {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advanceLocked(target)
	var out FuturesAsset
	if s.last != nil {
		out = FuturesAsset{P: ptr(s.last.Price), X: s.last.ExchangeMs}
	}
	s.cutoffMs = nextTarget
	s.advanceLocked(nextTarget)
	return out
}
