package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// auditSavedFuturesFiles verifies actual immutable Futures chunk contents.
// It never rewrites old snapshots, touches package manifests, or modifies
// Spot/Historical APIs. Inspect the oldest and four newest sealed chunks.
func auditSavedFuturesFiles(datasetDir string) {
	dir := filepath.Join(datasetDir, "chunks", "futures")
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("futures-sealed-audit read_error=%v", err)
		return
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".jsonl.gz") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	log.Printf("futures-sealed-audit chunks=%d", len(names))
	if len(names) == 0 {
		return
	}
	picked := []string{names[0]}
	for _, name := range names[maxInt(0, len(names)-4):] {
		if name != picked[0] {
			picked = append(picked, name)
		}
	}
	for _, name := range picked {
		rows, err := readGzipRows(filepath.Join(dir, name))
		if err != nil {
			log.Printf("futures-sealed-audit file=%s read_error=%v", name, err)
			continue
		}
		counts := [3]int{}
		var firstTs, lastTs int64
		invalid := 0
		for _, raw := range rows {
			var row FuturesSnapshot
			if err := json.Unmarshal(raw, &row); err != nil {
				invalid++
				continue
			}
			if firstTs == 0 {
				firstTs = row.T
			}
			lastTs = row.T
			if row.BTC.P != nil && *row.BTC.P > 0 && row.BTC.X > 0 {
				counts[0]++
			}
			if row.ETH.P != nil && *row.ETH.P > 0 && row.ETH.X > 0 {
				counts[1]++
			}
			if row.SOL.P != nil && *row.SOL.P > 0 && row.SOL.X > 0 {
				counts[2]++
			}
		}
		log.Printf("futures-sealed-audit file=%s rows=%d BTC_valid=%d ETH_valid=%d SOL_valid=%d malformed=%d firstT=%d lastT=%d",
			name, len(rows), counts[0], counts[1], counts[2], invalid, firstTs, lastTs)
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
