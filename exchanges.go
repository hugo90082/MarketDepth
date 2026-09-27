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

func binanceDeltaLevels(e timedBin, bidEdge, askEdge float64) ([]Level, []Level) {
	bids := make([]Level, 0, len(e.msg.B))
	asks := make([]Level, 0, len(e.msg.A))
	for _, x := range e.msg.B {
		if len(x) < 2 {
			continue
		}
		p, errP := strconv.ParseFloat(x[0], 64)
		q, errQ := strconv.ParseFloat(x[1], 64)
		if errP == nil && errQ == nil && p > 0 && bidEdge > 0 && p >= bidEdge {
			// Keep zero-quantity updates inside the trusted interval so existing
			// levels are deleted correctly.
			bids = append(bids, Level{Price: p, Qty: q})
		}
	}
	for _, x := range e.msg.A {
		if len(x) < 2 {
			continue
		}
		p, errP := strconv.ParseFloat(x[0], 64)
		q, errQ := strconv.ParseFloat(x[1], 64)
		if errP == nil && errQ == nil && p > 0 && askEdge > 0 && p <= askEdge {
			asks = append(asks, Level{Price: p, Qty: q})
		}
	}
	return bids, asks
}

func enqueueBinDelta(s *SourceState, e timedBin, bidEdge, askEdge float64) {
	bids, asks := binanceDeltaLevels(e, bidEdge, askEdge)
	// Enqueue even an empty filtered delta. A sequence-valid depth message proves
	// the local book stream is current, and apply("delta") advances depth freshness
	// without retaining any untrusted outer price levels.
	s.Enqueue(BookEvent{RecvMs: e.recv, Role: "broad", Kind: "delta", Bids: bids, Asks: asks})
}

func runBinance(ctx context.Context, asset string, s *SourceState, emit EventFn) {
	symbol := s.Pair
	stream := strings.ToLower(symbol) + "@depth"
	for {
		if ctx.Err() != nil {
			return
		}
		c, _, err := websocket.DefaultDialer.DialContext(ctx, "wss://stream.binance.com:9443/ws/"+stream, nil)
		if err != nil {
			emit(EventRecord{T: nowMs(), Venue: "binance", Asset: asset, Type: "disconnect", Code: "DIAL", Reason: err.Error()})
			sleepCtx(ctx, time.Second)
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

		snap, srecv, err := fetchBinanceSnapshot(ctx, symbol)
		if err != nil {
			c.Close()
			emit(EventRecord{T: nowMs(), Venue: "binance", Asset: asset, Type: "api_error", Code: "SNAPSHOT", Reason: err.Error()})
			sleepCtx(ctx, time.Second)
			continue
		}
		bids := map[float64]float64{}
		asks := map[float64]float64{}
		for _, x := range snap.Bids {
			p, _ := strconv.ParseFloat(x[0], 64)
			q, _ := strconv.ParseFloat(x[1], 64)
			if q > 0 {
				bids[p] = q
			}
		}
		for _, x := range snap.Asks {
			p, _ := strconv.ParseFloat(x[0], 64)
			q, _ := strconv.ParseFloat(x[1], 64)
			if q > 0 {
				asks[p] = q
			}
		}
		trustedBidEdge, trustedAskEdge := binanceTrustedEdges(bids, asks)
		last := snap.LastUpdateID
		avail := srecv
		buffered := []timedBin{}
		for {
			select {
			case e, ok := <-msgCh:
				if !ok {
					break
				}
				buffered = append(buffered, e)
			default:
				goto drained
			}
		}
	drained:
		bridgeOK := true
		for _, e := range buffered {
			if e.msg.LastUpdateID <= last {
				continue
			}
			if e.msg.FirstUpdateID > last+1 {
				bridgeOK = false
				break
			}
			applyBinLocal(bids, asks, e.msg)
			last = e.msg.LastUpdateID
			if e.recv > avail {
				avail = e.recv
			}
		}
		if !bridgeOK {
			c.Close()
			s.GapRole(nowMs(), "broad", "SEQUENCE_GAP")
			g, r := s.Counters()
			emit(EventRecord{T: nowMs(), Venue: "binance", Asset: asset, Type: "sequence_gap", Gaps: g, Resets: r})
			sleepCtx(ctx, time.Second)
			continue
		}
		// Buffered diff messages can contain isolated prices beyond the original
		// REST5000 edge. They are not coverage evidence and have no value for any
		// trusted zone, so do not carry them into the persistent in-memory book.
		bids, asks = pruneBinanceMapsToTrusted(bids, asks, trustedBidEdge, trustedAskEdge)
		s.SetTrustedEdges(trustedBidEdge, trustedAskEdge)
		s.Enqueue(BookEvent{RecvMs: avail, Role: "broad", Kind: "replace", Bids: mapLevels(bids), Asks: mapLevels(asks)})
		s.ResetCount()
		g, r := s.Counters()
		emit(EventRecord{T: avail, Venue: "binance", Asset: asset, Type: "reset_done", Gaps: g, Resets: r, Recovered: true})

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
				sleepCtx(ctx, time.Second)
				goto reconnect
			case e, ok := <-msgCh:
				if !ok {
					goto reconnect
				}
				if e.msg.LastUpdateID <= last {
					continue
				}
				if e.msg.FirstUpdateID > last+1 {
					c.Close()
					s.GapRole(e.recv, "broad", "SEQUENCE_GAP")
					g, r = s.Counters()
					emit(EventRecord{T: e.recv, Venue: "binance", Asset: asset, Type: "sequence_gap", Gaps: g, Resets: r})
					sleepCtx(ctx, time.Second)
					goto reconnect
				}
				enqueueBinDelta(s, e, trustedBidEdge, trustedAskEdge)
				last = e.msg.LastUpdateID
			}
		}
	reconnect:
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
