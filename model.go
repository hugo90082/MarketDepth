package main

import "time"

type CompactPair [2]*float64

// SpotSnapshot is intentionally compact: D[venue][asset][zone] = [bidQty, askQty].
type SpotSnapshot struct {
	T int64             `json:"t"`
	A int64             `json:"a"`
	D [][][]CompactPair `json:"d"`
}

type FuturesAsset struct {
	P *float64 `json:"p"`
	X int64    `json:"x,omitempty"`
}
type FuturesSnapshot struct {
	T   int64        `json:"t"`
	A   int64        `json:"a"`
	BTC FuturesAsset `json:"BTC"`
	ETH FuturesAsset `json:"ETH"`
	SOL FuturesAsset `json:"SOL"`
}

type EventRecord struct {
	T         int64  `json:"t"`
	Venue     string `json:"venue"`
	Asset     string `json:"asset"`
	Type      string `json:"type"`
	Code      string `json:"code,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Gaps      uint64 `json:"gaps,omitempty"`
	Resets    uint64 `json:"resets,omitempty"`
	Recovered bool   `json:"recovered,omitempty"`
}

type ChunkMeta struct {
	Kind     string `json:"kind"`
	ID       string `json:"id"`
	Rel      string `json:"rel"`
	ShaRel   string `json:"shaRel"`
	AuditRel string `json:"-"`
	StartMs  int64  `json:"startTsMs"`
	EndMs    int64  `json:"endTsMs"`
	Rows     int    `json:"rows"`
	Bytes    int64  `json:"bytes"`
	SHA256   string `json:"sha256"`
	Schema   string `json:"schema"`
}

type HistoryIndex struct {
	Schema    string      `json:"schema"`
	UpdatedMs int64       `json:"updatedTsMs"`
	Chunks    []ChunkMeta `json:"chunks"`
}

type PackageManifest struct {
	Schema         string         `json:"schema"`
	ID             string         `json:"id"`
	StartMs        int64          `json:"startTsMs"`
	EndMs          int64          `json:"endTsMs"`
	Bytes          int64          `json:"bytes"`
	Files          []string       `json:"files"`
	ChunkCounts    map[string]int `json:"chunkCounts,omitempty"`
	RowCounts      map[string]int `json:"rowCounts,omitempty"`
	Downloaded     bool           `json:"downloaded"`
	DownloadedAtMs int64          `json:"downloadedAtMs,omitempty"`
	SealedAtMs     int64          `json:"sealedAtMs"`
}

func ms(t time.Time) int64   { return t.UnixMilli() }
func ptr(v float64) *float64 { x := v; return &x }
