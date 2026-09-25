package main

import (
	"os"
	"strconv"
	"time"
)

type Zone struct{ Low, High float64 }

var (
	Venues = []string{"binance", "coinbase", "kraken", "bitfinex"}
	Assets = []string{"BTC", "ETH", "SOL"}
	Zones  = []Zone{{0, 100}, {100, 200}, {200, 300}, {300, 500}, {500, 750}}
)

const (
	DefaultSpotCadence = 30 * time.Second

	SchemaVersion      = "MARKET_DEPTH_COLLECTION_30S_5ZONE_1"
	SpotSchema         = "MD-SPOT-30S-1"
	FuturesSchema      = "MD-FUTURES-1S-1"
	EventSchema        = "MD-EVENT-1"
	PackageSchema      = "MD-PACKAGE-1"
	HistoryIndexSchema = "MD-HISTORY-INDEX-1"
)

type Config struct {
	DataDir              string
	DatasetDir           string
	Port                 string
	AdminPassword        string
	SessionSecret        string
	HistoryToken         string
	SpotCadence          time.Duration
	FuturesCadence       time.Duration
	FinalizeDelay        time.Duration
	ChunkDuration        time.Duration
	PackageDuration      time.Duration
	PackageMaxBytes      int64
	HistoryRatePerMinute int
	NoticeFreeBytes      int64
	WarningFreeBytes     int64
	UrgentFreeBytes      int64
	ProtectFreeBytes     int64
	StopFreeBytes        int64
}

func envInt64(k string, d int64) int64 {
	if v := os.Getenv(k); v != "" {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil {
			return n
		}
	}
	return d
}
func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			return n
		}
	}
	return d
}

func LoadConfig() Config {
	data := os.Getenv("DATA_DIR")
	if data == "" {
		data = "/data"
	}
	ds := os.Getenv("DATASET_DIR")
	if ds == "" {
		ds = data + "/market-depth"
	}
	p := os.Getenv("PORT")
	if p == "" {
		p = "8080"
	}
	return Config{
		DataDir: data, DatasetDir: ds, Port: p,
		AdminPassword: os.Getenv("ADMIN_PASSWORD"), SessionSecret: os.Getenv("SESSION_SECRET"), HistoryToken: os.Getenv("HISTORY_READ_TOKEN"),
		SpotCadence: DefaultSpotCadence, FuturesCadence: time.Second, FinalizeDelay: 100 * time.Millisecond,
		ChunkDuration: 15 * time.Minute, PackageDuration: time.Duration(envInt64("PACKAGE_DURATION_MS", int64((12*time.Hour)/time.Millisecond))) * time.Millisecond,
		PackageMaxBytes: envInt64("PACKAGE_MAX_BYTES", 60_000_000), HistoryRatePerMinute: envInt("HISTORY_RATE_PER_MINUTE", 30),
		NoticeFreeBytes:    envInt64("NOTICE_FREE_BYTES", 150_000_000), WarningFreeBytes: envInt64("WARNING_FREE_BYTES", 100_000_000),
		UrgentFreeBytes: envInt64("URGENT_FREE_BYTES", 50_000_000), ProtectFreeBytes: envInt64("PROTECT_FREE_BYTES", 25_000_000), StopFreeBytes: envInt64("STOP_FREE_BYTES", 10_000_000),
	}
}
