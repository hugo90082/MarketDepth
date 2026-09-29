package main

import (
	"context"
	"errors"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

func writeMeta(cfg Config) error {
	m := map[string]any{
		"schema": SchemaVersion, "spotSchema": SpotSchema, "futuresSchema": FuturesSchema, "eventSchema": EventSchema,
		"venues": Venues, "assets": Assets, "zonesBps": Zones, "spotCadenceMs": cfg.SpotCadence.Milliseconds(), "futuresCadenceMs": cfg.FuturesCadence.Milliseconds(),
		"timezone": "Asia/Taipei", "utcOffset": "+08:00", "historyAPI": "sealed immutable spot chunks + 16m recent window", "recentAPI": "/api/v1/recent/depth", "recentBufferMs": recentBufferDuration.Milliseconds(), "recentPollMinMs": int64(0), "apiRateLimitRequests": 2, "apiRateLimitWindowMs": int64(1000), "all4Stored": false,
		"depthUnit": DepthUnit, "quoteToUSDPolicy": QuoteToUSDPolicy,
		"spotAcquisition": "synchronized_30s_hybrid", "spotRequestLeadMs": spotRequestLead.Milliseconds(), "spotAcceptWindowMs": spotAcceptWindow.Milliseconds(),
		"spotSources": map[string]string{"binance": "BTC/ETH local book: REST5000 bootstrap + diff-depth 1000ms + U/u bridge; exact-T cut; full observed diff-level retention experiment; adaptive edge-headroom refresh plus periodic 15m rebootstrap; SOL one REST5000 snapshot", "coinbase": "one full level2 REST snapshot per 30s row", "kraken": "exact WS depth=1000 plus BTC/ETH GroupedBook depth=1000 grouping=1000 filling only uncovered zone sides; REST500 failure fallback", "bitfinex": "P0 near + P2 broad REST snapshots per 30s row"},
		"binanceCoveragePolicy": "BTC/ETH zone-side validity is gated only by the latest complete REST5000 bootstrap trusted bid/ask edges; diff-depth prices outside those edges are retained as observed-only levels but never authorize zone validity; trusted REST intervals refresh adaptively and at least every 15 minutes; SOL uses REST snapshot coverage",
		"missingSemantics": "coverage is independent per venue/asset/zone/side; confirmed empty=0; insufficient coverage or source failure=null; no fill/interpolation",
	}
	b, _ := json.MarshalIndent(m, "", "  ")
	return os.WriteFile(filepath.Join(cfg.DatasetDir, "meta", "schema.json"), b, 0644)
}

func pressure(cfg Config, free int64) string {
	switch {
	case free <= cfg.StopFreeBytes:
		return "STOP"
	case free <= cfg.ProtectFreeBytes:
		return "PROTECT"
	case free <= cfg.UrgentFreeBytes:
		return "URGENT"
	case free <= cfg.WarningFreeBytes:
		return "WARNING"
	case free <= cfg.NoticeFreeBytes:
		return "NOTICE"
	default:
		return "NORMAL"
	}
}

func main() {
	cfg := LoadConfig()
	if cfg.AdminPassword == "" || cfg.SessionSecret == "" || cfg.HistoryToken == "" {
		log.Fatal("ADMIN_PASSWORD, SESSION_SECRET and HISTORY_READ_TOKEN are required")
	}
	store, err := NewStore(cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err = writeMeta(cfg); err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := &RuntimeStatus{StartedMs: time.Now().UnixMilli()}
	recent := NewRecentBuffer(recentBufferDuration)
	if err := recent.RebuildFromStore(store, nowMs()); err != nil {
		log.Printf("recent-buffer-rebuild warning=%v", err)
	} else {
		rows, oldest, latest := recent.RowsSince(0)
		log.Printf("recent-buffer-rebuild rows=%d oldest=%d latest=%d", len(rows), oldest, latest)
	}
	rt.Recent = recent
	emit := func(e EventRecord) {
		if e.T == 0 {
			e.T = nowMs()
		}
		if err := store.Add("events", e.T, e); err == nil {
			rt.EventRows.Add(1)
		}
	}
	feeds := StartFeeds(ctx, emit)
	rt.Feeds = feeds
	spotSampler := NewSpotSampler(emit, feeds.Sources, cfg.SpotCadence)
	firstSpot := time.Now().Truncate(cfg.SpotCadence).Add(cfg.SpotCadence)
	for _, a := range []string{"BTC", "ETH"} {
		if s := feeds.Sources[sourceKey("binance", a)]; s != nil {
			s.SetCutoff(firstSpot.UnixMilli())
		}
	}
	firstFuture := time.Now().Truncate(cfg.FuturesCadence).Add(cfg.FuturesCadence)
	for _, s := range feeds.Futures {
		s.SetCutoff(firstFuture.UnixMilli())
	}
	srv := StartServer(cfg, store, rt)
	log.Printf("marketdepth-start schema=%s dataset=%s", SchemaVersion, cfg.DatasetDir)

	go spotLoop(ctx, cfg, store, rt, spotSampler, emit, firstSpot)
	go futuresLoop(ctx, cfg, store, rt, feeds, firstFuture)
	go flushLoop(ctx, cfg, store, rt, emit)
	go statusLoop(ctx, cfg, store, rt)

	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	feeds.Stop()
	_ = srv.Shutdown(context.Background())
	_ = store.CheckpointOpen()
	log.Printf("marketdepth-stop")
}

func spotLoop(ctx context.Context, cfg Config, store *Store, rt *RuntimeStatus, sampler *SpotSampler, emit EventFn, next time.Time) {
	for {
		if !sleepUntil(ctx, next.Add(-spotRequestLead)) {
			return
		}
		target := next.UnixMilli()
		_, free, _, _ := store.Disk()
		p := pressure(cfg, free)
		if p == "STOP" {
			emit(EventRecord{T: target, Venue: "collector", Asset: "ALL", Type: "disk_stop", Code: "STOP"})
			next = next.Add(cfg.SpotCadence)
			continue
		}

		// If the process wakes after the accepted T+1.5s window, never fetch a
		// current book and pretend it belonged to the missed target. Persist an
		// all-null row so the time grid remains explicit and auditable.
		if time.Now().After(next.Add(spotAcceptWindow)) {
			now := nowMs()
			row := SpotSnapshot{T: target, A: now, D: emptySpotGrid()}
			if err := store.Add("spot", target, row); err != nil {
				if !errors.Is(err, ErrStorageStopped) {
					rt.LastError.Store(err.Error())
				}
			} else {
				rt.SpotRows.Add(1)
				if rt.Recent != nil {
					rt.Recent.Add(row)
				}
			}
			emit(EventRecord{T: target, Venue: "collector", Asset: "ALL", Type: "scheduler_miss", Reason: "missed_rest_snapshot_window"})
			next = next.Add(cfg.SpotCadence)
			continue
		}

		d, available := sampler.Sample(ctx, target)
		row := SpotSnapshot{T: target, A: available, D: d}
		if err := store.Add("spot", target, row); err != nil {
			if !errors.Is(err, ErrStorageStopped) {
				rt.LastError.Store(err.Error())
			}
		} else {
			rt.SpotRows.Add(1)
			if rt.Recent != nil {
				rt.Recent.Add(row)
			}
		}
		next = next.Add(cfg.SpotCadence)
	}
}

func futuresLoop(ctx context.Context, cfg Config, store *Store, rt *RuntimeStatus, feeds *Feeds, next time.Time) {
	for {
		if !sleepUntil(ctx, next.Add(cfg.FinalizeDelay)) {
			return
		}
		t := next.UnixMilli()
		nextTarget := next.Add(cfg.FuturesCadence).UnixMilli()
		row := FuturesSnapshot{T: t, A: nowMs(), BTC: feeds.Futures["BTC"].AtAdvance(t, nextTarget), ETH: feeds.Futures["ETH"].AtAdvance(t, nextTarget), SOL: feeds.Futures["SOL"].AtAdvance(t, nextTarget)}
		if err := store.Add("futures", t, row); err != nil {
			if !errors.Is(err, ErrStorageStopped) {
				rt.LastError.Store(err.Error())
			}
		} else {
			rt.FuturesRows.Add(1)
		}
		next = next.Add(time.Second)
	}
}

func flushLoop(ctx context.Context, cfg Config, store *Store, rt *RuntimeStatus, emit EventFn) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	nextCheckpoint := time.Now().Add(time.Minute)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			_, free, _, diskErr := store.Disk()
			if diskErr == nil {
				store.SetWritesStopped(pressure(cfg, free) == "STOP")
			}
			ms, err := store.FlushClosed(nowMs())
			if err != nil {
				rt.LastError.Store(err.Error())
				continue
			}
			if len(ms) > 0 {
				emit(EventRecord{T: nowMs(), Venue: "collector", Asset: "ALL", Type: "chunk_flush", Reason: fmt.Sprintf("files=%d", len(ms))})
			}
			if !time.Now().Before(nextCheckpoint) {
				if err := store.CheckpointOpen(); err != nil {
					rt.LastError.Store(err.Error())
				}
				nextCheckpoint = time.Now().Add(time.Minute)
			}
		}
	}
}
func statusLoop(ctx context.Context, cfg Config, store *Store, rt *RuntimeStatus) {
	tick := time.NewTicker(60 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			total, free, used, _ := store.Disk()
			errStr := ""
			if x := rt.LastError.Load(); x != nil {
				errStr = x.(string)
			}
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			log.Printf("marketdepth-status uptimeSec=%d spotRows=%d futuresRows=%d eventRows=%d diskTotal=%d diskUsed=%d diskFree=%d pressure=%s lastFlushMs=%d heapAllocBytes=%d heapInuseBytes=%d heapSysBytes=%d stackInuseBytes=%d numGC=%d lastError=%q", (nowMs()-rt.StartedMs)/1000, rt.SpotRows.Load(), rt.FuturesRows.Load(), rt.EventRows.Load(), total, used, free, pressure(cfg, free), store.LastFlush(), ms.HeapAlloc, ms.HeapInuse, ms.HeapSys, ms.StackInuse, ms.NumGC, errStr)
			if rt.Feeds != nil {
				t := nowMs()
				cov := map[string]CoverageAudit{}
				for _, a := range []string{"BTC", "ETH"} {
					if s := rt.Feeds.Sources[sourceKey("binance", a)]; s != nil {
						cov[a] = s.CoverageAudit(t)
					}
				}
				if b, e := json.Marshal(cov); e == nil {
					log.Printf("binance-coverage-audit %s", b)
				}
			}
		}
	}
}
