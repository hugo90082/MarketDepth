package main

import (
	"testing"
	"time"
)

func TestRecentFuturesBufferSinceAndPrune(t *testing.T) {
	b := NewRecentFuturesBuffer(2 * time.Second)
	for i := int64(1); i <= 5; i++ {
		b.Add(FuturesSnapshot{T: i * 1000, BTC: FuturesAsset{P: ptr(float64(i)), X: i * 1000}})
	}
	rows, oldest, latest := b.RowsSince(0)
	if len(rows) != 3 || oldest != 3000 || latest != 5000 {
		t.Fatalf("bad window len=%d oldest=%d latest=%d", len(rows), oldest, latest)
	}
	inc, _, _ := b.RowsSince(4000)
	if len(inc) != 1 || inc[0].BTC.P == nil || *inc[0].BTC.P != 5 {
		t.Fatalf("bad futures incremental result: %#v", inc)
	}
}
