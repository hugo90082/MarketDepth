package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type EventFn func(EventRecord)

type Feeds struct {
	Sources map[string]*SourceState
	Futures map[string]*FutureState
	cancel  context.CancelFunc
}

func sourceKey(v, a string) string { return v + ":" + a }
func nowMs() int64                 { return time.Now().UnixMilli() }
func StartFeeds(parent context.Context, emit EventFn) *Feeds {
	ctx, cancel := context.WithCancel(parent)
	src := map[string]*SourceState{
		sourceKey("binance", "BTC"): NewSource("binance", "BTC", "BTCUSDT"),
		sourceKey("binance", "ETH"): NewSource("binance", "ETH", "ETHUSDT"),
	}
	fut := map[string]*FutureState{"BTC": {}, "ETH": {}, "SOL": {}}
	f := &Feeds{Sources: src, Futures: fut, cancel: cancel}

	// Only Binance BTC/ETH require a persistent local book. All other Spot
	// sources are sampled by the unified 30-second SpotSampler path.
	for _, a := range []string{"BTC", "ETH"} {
		go runBinance(ctx, a, src[sourceKey("binance", a)], emit)
	}
	go runBinanceFutures(ctx, fut, emit)
	return f
}

func (f *Feeds) Stop() { f.cancel() }

// ---------- Binance Spot ----------

type binDepth struct {
	FirstUpdateID uint64     `json:"U"`
	LastUpdateID  uint64     `json:"u"`
	B             [][]string `json:"b"`
	A             [][]string `json:"a"`
}

type binSnap struct {
	LastUpdateID uint64     `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

type timedBin struct {
	recv int64
	msg  binDepth
}

func fetchBinanceSnapshot(ctx context.Context, symbol string) (binSnap, int64, error) {
	var out binSnap
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://api.binance.com/api/v3/depth?symbol="+url.QueryEscape(symbol)+"&limit=5000", nil)
	cli := &http.Client{Timeout: 10 * time.Second}
	res, err := cli.Do(req)
	if err != nil {
		return out, 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		return out, 0, fmt.Errorf("snapshot %d %s", res.StatusCode, string(b))
	}
	err = json.NewDecoder(res.Body).Decode(&out)
	return out, nowMs(), err
}

func applyBinLocal(bids, asks map[float64]float64, e binDepth) {
	for _, x := range e.B {
		if len(x) < 2 {
			continue
		}
		p, _ := strconv.ParseFloat(x[0], 64)
		q, _ := strconv.ParseFloat(x[1], 64)
		if q <= 0 {
			delete(bids, p)
		} else {
			bids[p] = q
		}
	}
	for _, x := range e.A {
		if len(x) < 2 {
			continue
		}
		p, _ := strconv.ParseFloat(x[0], 64)
		q, _ := strconv.ParseFloat(x[1], 64)
		if q <= 0 {
			delete(asks, p)
		} else {
			asks[p] = q
		}
	}
}

func binanceTrustedEdges(bids, asks map[float64]float64) (float64, float64) {
	minBid := 0.0
	maxAsk := 0.0
	for p, q := range bids {
		if q <= 0 {
			continue
		}
		if minBid == 0 || p < minBid {
			minBid = p
		}
	}
	for p, q := range asks {
		if q <= 0 {
			continue
		}
		if p > maxAsk {
			maxAsk = p
		}
	}
	return minBid, maxAsk
}

func mapLevels(m map[float64]float64) []Level {
	r := make([]Level, 0, len(m))
	for p, q := range m {
		if q > 0 {
			r = append(r, Level{p, q})
		}
	}
	return r
}

// pruneBinanceMapsToTrusted rebuilds the local bootstrap maps using only the
// absolute price interval proven complete by REST5000. Rebuilding (rather than
// deleting in-place) lets Go release oversized map bucket arrays after a later GC.
func pruneBinanceMapsToTrusted(bids, asks map[float64]float64, bidEdge, askEdge float64) (map[float64]float64, map[float64]float64) {
	nb := make(map[float64]float64, len(bids))
	na := make(map[float64]float64, len(asks))
	if bidEdge > 0 {
		for p, q := range bids {
			if q > 0 && p >= bidEdge {
				nb[p] = q
			}
		}
	}
	if askEdge > 0 {
		for p, q := range asks {
			if q > 0 && p <= askEdge {
				na[p] = q
			}
		}
	}
	return nb, na
}

func binanceDeltaLevels(e timedBin) ([]Level, []Level) {
	bids := make([]Level, 0, len(e.msg.B))
	asks := make([]Level, 0, len(e.msg.A))
	for _, x := range e.msg.B {
		if len(x) < 2 {
			continue
		}
		p, errP := strconv.ParseFloat(x[0], 64)
		q, errQ := strconv.ParseFloat(x[1], 64)
		if errP == nil && errQ == nil && p > 0 {
			// Keep every observed price level. Qty=0 must also be retained as
			// an update so stale levels are actually deleted from the book.
			bids = append(bids, Level{Price: p, Qty: q})
		}
	}
	for _, x := range e.msg.A {
		if len(x) < 2 {
			continue
		}
		p, errP := strconv.ParseFloat(x[0], 64)
		q, errQ := strconv.ParseFloat(x[1], 64)
		if errP == nil && errQ == nil && p > 0 {
			asks = append(asks, Level{Price: p, Qty: q})
		}
	}
	return bids, asks
}

func enqueueBinDelta(s *SourceState, e timedBin) {
	bids, asks := binanceDeltaLevels(e)
	s.Enqueue(BookEvent{RecvMs: e.recv, Role: "broad", Kind: "delta", Bids: bids, Asks: asks})
}

const binanceBootstrapMaxAge = 15 * time.Minute

func binanceRefreshFloor(initialCoverage float64) float64 {
	if initialCoverage <= 0 {
		return 0
	}
	f := initialCoverage * 0.25
	if f < 1 {
		f = 1
	}
	if f > 25 {
		f = 25
	}
	if f >= initialCoverage {
		f = initialCoverage * 0.5
	}
	return f
}

func binanceCoverageFromEdges(mid, bidEdge, askEdge float64) (float64, float64) {
	if mid <= 0 {
		return 0, 0
	}
	bid, ask := 0.0, 0.0
	if bidEdge > 0 && bidEdge < mid {
		bid = (mid - bidEdge) / mid * 10000
	}
	if askEdge > mid {
		ask = (askEdge - mid) / mid * 10000
	}
	return bid, ask
}

func binanceMapMid(bids, asks map[float64]float64) float64 {
	bestBid, bestAsk := 0.0, 0.0
	for p, q := range bids {
		if q > 0 && p > bestBid {
			bestBid = p
		}
	}
	for p, q := range asks {
		if q <= 0 {
			continue
		}
		if bestAsk == 0 || p < bestAsk {
			bestAsk = p
		}
	}
	if bestBid <= 0 || bestAsk <= 0 {
		return 0
	}
	return (bestBid + bestAsk) / 2
}

type binanceBootstrap struct {
	bids, asks             map[float64]float64
	trustedBid, trustedAsk float64
	last                   uint64
	avail                  int64
	initialBidBps          float64
	initialAskBps          float64
}

func prepareBinanceBootstrap(ctx context.Context, symbol string, msgCh <-chan timedBin) (binanceBootstrap, error) {
	var out binanceBootstrap
	snap, srecv, err := fetchBinanceSnapshot(ctx, symbol)
	if err != nil {
		return out, err
	}
	bids := map[float64]float64{}
	asks := map[float64]float64{}
	for _, x := range snap.Bids {
		if len(x) < 2 {
			continue
		}
		p, errP := strconv.ParseFloat(x[0], 64)
		q, errQ := strconv.ParseFloat(x[1], 64)
		if errP == nil && errQ == nil && p > 0 && q > 0 {
			bids[p] = q
		}
	}
	for _, x := range snap.Asks {
		if len(x) < 2 {
			continue
		}
		p, errP := strconv.ParseFloat(x[0], 64)
		q, errQ := strconv.ParseFloat(x[1], 64)
		if errP == nil && errQ == nil && p > 0 && q > 0 {
			asks[p] = q
		}
	}
	trustedBid, trustedAsk := binanceTrustedEdges(bids, asks)
	last := snap.LastUpdateID
	avail := srecv

	// Keep draining everything already buffered while the REST snapshot was in
	// flight. A closed channel must terminate the bootstrap rather than spin.
	for {
		select {
		case e, ok := <-msgCh:
			if !ok {
				return out, io.EOF
			}
			if e.msg.LastUpdateID <= last {
				continue
			}
			if e.msg.FirstUpdateID > last+1 {
				return out, fmt.Errorf("sequence gap during bootstrap: U=%d last=%d", e.msg.FirstUpdateID, last)
			}
			applyBinLocal(bids, asks, e.msg)
			last = e.msg.LastUpdateID
			if e.recv > avail {
				avail = e.recv
			}
		default:
			mid := binanceMapMid(bids, asks)
			bidBps, askBps := binanceCoverageFromEdges(mid, trustedBid, trustedAsk)
			out = binanceBootstrap{
				bids: bids, asks: asks,
				trustedBid: trustedBid, trustedAsk: trustedAsk,
				last: last, avail: avail,
				initialBidBps: bidBps, initialAskBps: askBps,
			}
			return out, nil
		}
	}
}

func nextBinanceRetry(d time.Duration) time.Duration {
	d *= 2
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

func runBinance(ctx context.Context, asset string, s *SourceState, emit EventFn) {
	symbol := s.Pair
	stream := strings.ToLower(symbol) + "@depth"
	retryDelay := time.Second

	for {
		if ctx.Err() != nil {
			return
		}
		c, _, err := websocket.DefaultDialer.DialContext(ctx, "wss://stream.binance.com:9443/ws/"+stream, nil)
		if err != nil {
			emit(EventRecord{T: nowMs(), Venue: "binance", Asset: asset, Type: "disconnect", Code: "DIAL", Reason: err.Error()})
			log.Printf("binance-%s reconnect dial_error retry=%s err=%v", asset, retryDelay, err)
			sleepCtx(ctx, retryDelay)
			retryDelay = nextBinanceRetry(retryDelay)
			continue
		}

		defaultPing := c.PingHandler()
		c.SetPingHandler(func(appData string) error {
			s.TouchRole(nowMs(), "broad")
			return defaultPing(appData)
		})

		msgCh := make(chan timedBin, 4096)
		errCh := make(chan error, 1)
		go func() {
			defer close(msgCh)
			for {
				_, b, e := c.ReadMessage()
				if e != nil {
					select {
					case errCh <- e:
					default:
					}
					return
				}
				var m binDepth
				if json.Unmarshal(b, &m) == nil && m.LastUpdateID > 0 {
					select {
					case msgCh <- timedBin{recv: nowMs(), msg: m}:
					case <-ctx.Done():
						return
					}
				}
			}
		}()

		boot, err := prepareBinanceBootstrap(ctx, symbol, msgCh)
		if err != nil {
			c.Close()
			emit(EventRecord{T: nowMs(), Venue: "binance", Asset: asset, Type: "api_error", Code: "BOOTSTRAP", Reason: err.Error()})
			log.Printf("binance-%s bootstrap_failed retry=%s err=%v", asset, retryDelay, err)
			sleepCtx(ctx, retryDelay)
			retryDelay = nextBinanceRetry(retryDelay)
			continue
		}

		s.SetTrustedEdges(boot.trustedBid, boot.trustedAsk)
		s.Enqueue(BookEvent{RecvMs: boot.avail, Role: "broad", Kind: "replace", Bids: mapLevels(boot.bids), Asks: mapLevels(boot.asks)})
		// Drop bootstrap construction maps after converting to compact levels so
		// the persistent SourceState book is the only long-lived copy.
		boot.bids, boot.asks = nil, nil
		s.ResetCount()
		g, r := s.Counters()
		emit(EventRecord{T: boot.avail, Venue: "binance", Asset: asset, Type: "reset_done", Gaps: g, Resets: r, Recovered: true})

		last := boot.last
		bidFloor := binanceRefreshFloor(boot.initialBidBps)
		askFloor := binanceRefreshFloor(boot.initialAskBps)
		bootAt := time.Now()
		nextCoverageCheck := time.Now().Add(5 * time.Second)
		retryDelay = time.Second

		log.Printf("binance-%s bootstrap_ok bidBps=%.2f askBps=%.2f bidFloor=%.2f askFloor=%.2f", asset, boot.initialBidBps, boot.initialAskBps, bidFloor, askFloor)

	live:
		for {
			select {
			case <-ctx.Done():
				c.Close()
				return
			case err = <-errCh:
				c.Close()
				s.GapRole(nowMs(), "broad", "DISCONNECTED")
				g, r = s.Counters()
				emit(EventRecord{T: nowMs(), Venue: "binance", Asset: asset, Type: "disconnect", Reason: err.Error(), Gaps: g, Resets: r})
				log.Printf("binance-%s disconnected retry=%s err=%v", asset, retryDelay, err)
				sleepCtx(ctx, retryDelay)
				retryDelay = nextBinanceRetry(retryDelay)
				break live
			case e, ok := <-msgCh:
				if !ok {
					c.Close()
					s.GapRole(nowMs(), "broad", "DISCONNECTED")
					log.Printf("binance-%s websocket_closed retry=%s", asset, retryDelay)
					sleepCtx(ctx, retryDelay)
					retryDelay = nextBinanceRetry(retryDelay)
					break live
				}
				if e.msg.LastUpdateID <= last {
					continue
				}
				if e.msg.FirstUpdateID > last+1 {
					c.Close()
					s.GapRole(e.recv, "broad", "SEQUENCE_GAP")
					g, r = s.Counters()
					emit(EventRecord{T: e.recv, Venue: "binance", Asset: asset, Type: "sequence_gap", Gaps: g, Resets: r})
					log.Printf("binance-%s sequence_gap U=%d last=%d retry=%s", asset, e.msg.FirstUpdateID, last, retryDelay)
					sleepCtx(ctx, retryDelay)
					retryDelay = nextBinanceRetry(retryDelay)
					break live
				}
				enqueueBinDelta(s, e)
				last = e.msg.LastUpdateID

				if time.Now().Before(nextCoverageCheck) {
					continue
				}
				nextCoverageCheck = time.Now().Add(5 * time.Second)

				ready, bidBps, askBps := s.TrustedCoverageNow()
				reason := ""
				switch {
				case !ready:
					reason = "book_side_depleted"
				case bidFloor > 0 && bidBps < bidFloor:
					reason = fmt.Sprintf("bid_headroom_%.2f_bps", bidBps)
				case askFloor > 0 && askBps < askFloor:
					reason = fmt.Sprintf("ask_headroom_%.2f_bps", askBps)
				case time.Since(bootAt) >= binanceBootstrapMaxAge:
					reason = "periodic_15m"
				}
				if reason == "" {
					continue
				}

				// Refresh against the same live WebSocket. The reader goroutine
				// buffers diff messages while REST5000 is in flight; the helper
				// bridges them onto the new snapshot. The trusted REST interval
				// is replaced authoritatively while observed outer levels remain
				// sequence-maintained for the full-retention RAM experiment.
				log.Printf("binance-%s rebootstrap_start reason=%s bidBps=%.2f askBps=%.2f", asset, reason, bidBps, askBps)
				fresh, refreshErr := prepareBinanceBootstrap(ctx, symbol, msgCh)
				if refreshErr != nil {
					c.Close()
					s.GapRole(nowMs(), "broad", "REBOOTSTRAP_FAILED")
					g, r = s.Counters()
					emit(EventRecord{T: nowMs(), Venue: "binance", Asset: asset, Type: "api_error", Code: "REBOOTSTRAP", Reason: refreshErr.Error(), Gaps: g, Resets: r})
					log.Printf("binance-%s rebootstrap_failed reason=%s retry=%s err=%v", asset, reason, retryDelay, refreshErr)
					sleepCtx(ctx, retryDelay)
					retryDelay = nextBinanceRetry(retryDelay)
					break live
				}

				s.SetTrustedEdges(fresh.trustedBid, fresh.trustedAsk)
				s.Enqueue(BookEvent{
					RecvMs: fresh.avail, Role: "broad", Kind: "refresh",
					Bids: mapLevels(fresh.bids), Asks: mapLevels(fresh.asks),
					BidEdge: fresh.trustedBid, AskEdge: fresh.trustedAsk,
				})
				fresh.bids, fresh.asks = nil, nil
				s.ResetCount()
				last = fresh.last
				bidFloor = binanceRefreshFloor(fresh.initialBidBps)
				askFloor = binanceRefreshFloor(fresh.initialAskBps)
				bootAt = time.Now()
				retryDelay = time.Second
				emit(EventRecord{T: fresh.avail, Venue: "binance", Asset: asset, Type: "reset_done", Code: "REBOOTSTRAP", Recovered: true})
				log.Printf("binance-%s rebootstrap_ok reason=%s bidBps=%.2f askBps=%.2f bidFloor=%.2f askFloor=%.2f", asset, reason, fresh.initialBidBps, fresh.initialAskBps, bidFloor, askFloor)
			}
		}
	}
}

// ---------- Binance Futures ----------

func runBinanceFutures(ctx context.Context, f map[string]*FutureState, emit EventFn) {
	streams := []string{"btcusdt@aggTrade", "ethusdt@aggTrade", "solusdt@aggTrade"}
	for {
		if ctx.Err() != nil {
			return
		}
		c, _, err := websocket.DefaultDialer.DialContext(ctx, "wss://fstream.binance.com/stream?streams="+strings.Join(streams, "/"), nil)
		if err != nil {
			sleepCtx(ctx, time.Second)
			continue
		}
		for {
			_, b, e := c.ReadMessage()
			if e != nil {
				c.Close()
				emit(EventRecord{T: nowMs(), Venue: "binance_futures", Asset: "ALL", Type: "disconnect", Reason: e.Error()})
				break
			}
			recv := nowMs()
			var m struct {
				Data struct {
					Symbol  string `json:"s"`
					Price   string `json:"p"`
					TradeMs int64  `json:"T"`
				} `json:"data"`
			}
			if json.Unmarshal(b, &m) != nil {
				continue
			}
			asset := ""
			switch m.Data.Symbol {
			case "BTCUSDT":
				asset = "BTC"
			case "ETHUSDT":
				asset = "ETH"
			case "SOLUSDT":
				asset = "SOL"
			}
			if asset == "" {
				continue
			}
			p, _ := strconv.ParseFloat(m.Data.Price, 64)
			f[asset].Add(TradeEvent{RecvMs: recv, ExchangeMs: m.Data.TradeMs, Price: p})
		}
		sleepCtx(ctx, time.Second)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d < 0 {
		d = 0
	}
	return sleepCtx(ctx, d)
}
func init() { log.SetFlags(log.LstdFlags | log.LUTC) }
