package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	spotRequestLead  = 1500 * time.Millisecond
	spotAcceptWindow = 1500 * time.Millisecond
)

type SpotSampler struct {
	client   *http.Client
	emit     EventFn
	sources  map[string]*SourceState
	cadence  time.Duration
	mu       sync.Mutex
	last     map[string]string
}

type spotRESTResult struct {
	venue  string
	asset  string
	recvMs int64
	zones  []CompactPair
	err    error
}

func NewSpotSampler(emit EventFn, sources map[string]*SourceState, cadence time.Duration) *SpotSampler {
	if cadence <= 0 {
		cadence = DefaultSpotCadence
	}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 1200 * time.Millisecond, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   1200 * time.Millisecond,
		ExpectContinueTimeout: 500 * time.Millisecond,
	}
	return &SpotSampler{
		client:  &http.Client{Transport: tr},
		emit:     emit,
		sources:  sources,
		cadence:  cadence,
		last:     map[string]string{},
	}
}

func emptySpotGrid() [][][]CompactPair {
	d := make([][][]CompactPair, len(Venues))
	for vi := range d {
		d[vi] = make([][]CompactPair, len(Assets))
		for ai := range d[vi] {
			d[vi][ai] = make([]CompactPair, len(Zones))
		}
	}
	return d
}

func withinSpotWindow(targetMs, recvMs int64) bool {
	w := spotAcceptWindow.Milliseconds()
	return recvMs >= targetMs-w && recvMs <= targetMs+w
}

func completeZones(z []CompactPair) bool {
	if len(z) != len(Zones) {
		return false
	}
	for i := range z {
		if z[i][0] == nil || z[i][1] == nil {
			return false
		}
	}
	return true
}

func zonesFromLevels(bids, asks []Level) []CompactPair {
	out := make([]CompactPair, len(Zones))
	bb, ba := 0.0, 0.0
	for _, x := range bids {
		if x.Price > bb && x.Qty > 0 {
			bb = x.Price
		}
	}
	for _, x := range asks {
		if x.Price > 0 && x.Qty > 0 && (ba == 0 || x.Price < ba) {
			ba = x.Price
		}
	}
	if bb <= 0 || ba <= 0 || bb >= ba {
		return out
	}
	mid := (bb + ba) / 2
	minBid, maxAsk := bb, ba
	for _, x := range bids {
		if x.Qty > 0 && x.Price > 0 && x.Price < minBid {
			minBid = x.Price
		}
	}
	for _, x := range asks {
		if x.Qty > 0 && x.Price > maxAsk {
			maxAsk = x.Price
		}
	}
	bidCoverage := (mid - minBid) / mid * 10000
	askCoverage := (maxAsk - mid) / mid * 10000
	bidSums := make([]float64, len(Zones))
	askSums := make([]float64, len(Zones))
	for _, x := range bids {
		if x.Qty <= 0 || x.Price <= 0 {
			continue
		}
		d := (mid - x.Price) / mid * 10000
		for zi, z := range Zones {
			if d >= z.Low && d < z.High {
				bidSums[zi] += x.Price * x.Qty
				break
			}
		}
	}
	for _, x := range asks {
		if x.Qty <= 0 || x.Price <= 0 {
			continue
		}
		d := (x.Price - mid) / mid * 10000
		for zi, z := range Zones {
			if d >= z.Low && d < z.High {
				askSums[zi] += x.Price * x.Qty
				break
			}
		}
	}
	for zi, z := range Zones {
		if bidCoverage+1e-9 >= z.High {
			out[zi][0] = ptr(bidSums[zi])
		}
		if askCoverage+1e-9 >= z.High {
			out[zi][1] = ptr(askSums[zi])
		}
	}
	return out
}

func mergeBitfinexZones(p1, p2 []CompactPair) []CompactPair {
	out := make([]CompactPair, len(Zones))
	for i := range Zones {
		if Zones[i].High <= 100 {
			if len(p1) > i {
				out[i] = p1[i]
			}
			continue
		}
		if len(p2) > i {
			out[i] = p2[i]
		}
	}
	return out
}

func parseStringLevels(rows [][]string) []Level {
	out := make([]Level, 0, len(rows))
	for _, x := range rows {
		if len(x) < 2 {
			continue
		}
		p, err1 := strconv.ParseFloat(x[0], 64)
		q, err2 := strconv.ParseFloat(x[1], 64)
		if err1 == nil && err2 == nil && p > 0 && q > 0 {
			out = append(out, Level{Price: p, Qty: q})
		}
	}
	return out
}

func parseRawLevels(rows [][]json.RawMessage) []Level {
	out := make([]Level, 0, len(rows))
	for _, x := range rows {
		if len(x) < 2 {
			continue
		}
		ps := strings.Trim(string(x[0]), "\"")
		qs := strings.Trim(string(x[1]), "\"")
		p, err1 := strconv.ParseFloat(ps, 64)
		q, err2 := strconv.ParseFloat(qs, 64)
		if err1 == nil && err2 == nil && p > 0 && q > 0 {
			out = append(out, Level{Price: p, Qty: q})
		}
	}
	return out
}

func (s *SpotSampler) getJSON(ctx context.Context, u string, out any) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "marketdepth-rest/1")
	res, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("http_status=%d", res.StatusCode)
	}
	if err := json.NewDecoder(res.Body).Decode(out); err != nil {
		return 0, err
	}
	return nowMs(), nil
}

func (s *SpotSampler) fetchBinanceSOL(ctx context.Context) ([]CompactPair, int64, error) {
	var r struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
	}
	// SOL is sparse enough that the public 5000-level snapshot covers the
	// configured 0-750 bps target. Fetch it once instead of doing 1000 then
	// retrying 5000 every 30 seconds.
	u := "https://api.binance.com/api/v3/depth?symbol=SOLUSDT&limit=5000"
	recv, err := s.getJSON(ctx, u, &r)
	if err != nil {
		return nil, recv, err
	}
	return zonesFromLevels(parseStringLevels(r.Bids), parseStringLevels(r.Asks)), recv, nil
}

func (s *SpotSampler) fetchCoinbase(ctx context.Context, asset string) ([]CompactPair, int64, error) {
	products := map[string]string{"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD"}
	var r struct {
		Bids [][]json.RawMessage `json:"bids"`
		Asks [][]json.RawMessage `json:"asks"`
	}
	u := "https://api.exchange.coinbase.com/products/" + url.PathEscape(products[asset]) + "/book?level=2"
	recv, err := s.getJSON(ctx, u, &r)
	if err != nil {
		return nil, recv, err
	}
	return zonesFromLevels(parseRawLevels(r.Bids), parseRawLevels(r.Asks)), recv, nil
}

func parseKrakenGroupedLevels(rows []struct {
	Price string `json:"price"`
	Qty   string `json:"qty"`
}) []Level {
	out := make([]Level, 0, len(rows))
	for _, x := range rows {
		p, errP := strconv.ParseFloat(x.Price, 64)
		q, errQ := strconv.ParseFloat(x.Qty, 64)
		if errP == nil && errQ == nil && p > 0 && q > 0 {
			out = append(out, Level{Price: p, Qty: q})
		}
	}
	return out
}

func mergePreferExact(exact, grouped []CompactPair) []CompactPair {
	out := make([]CompactPair, len(Zones))
	for i := range Zones {
		if i < len(exact) {
			out[i] = exact[i]
		}
		if i >= len(grouped) {
			continue
		}
		if out[i][0] == nil {
			out[i][0] = grouped[i][0]
		}
		if out[i][1] == nil {
			out[i][1] = grouped[i][1]
		}
	}
	return out
}

func (s *SpotSampler) fetchKrakenGrouped(ctx context.Context, asset string) ([]CompactPair, int64, error) {
	pairs := map[string]string{"BTC": "XBTUSD", "ETH": "ETHUSD", "SOL": "SOLUSD"}
	var r struct {
		Error  []string `json:"error"`
		Result struct {
			Pair     string `json:"pair"`
			Grouping int    `json:"grouping"`
			Bids []struct {
				Price string `json:"price"`
				Qty   string `json:"qty"`
			} `json:"bids"`
			Asks []struct {
				Price string `json:"price"`
				Qty   string `json:"qty"`
			} `json:"asks"`
		} `json:"result"`
	}
	u := "https://api.kraken.com/0/public/GroupedBook?pair=" + url.QueryEscape(pairs[asset]) + "&depth=1000&grouping=1000"
	recv, err := s.getJSON(ctx, u, &r)
	if err != nil {
		return nil, recv, err
	}
	if len(r.Error) > 0 {
		return nil, recv, fmt.Errorf("kraken_grouped=%s", strings.Join(r.Error, ","))
	}
	bids := parseKrakenGroupedLevels(r.Result.Bids)
	asks := parseKrakenGroupedLevels(r.Result.Asks)
	if len(bids) == 0 || len(asks) == 0 {
		return nil, recv, fmt.Errorf("kraken_grouped_empty")
	}
	return zonesFromLevels(bids, asks), recv, nil
}

func (s *SpotSampler) fetchKrakenREST(ctx context.Context, asset string) ([]CompactPair, int64, error) {
	pairs := map[string]string{"BTC": "XBTUSD", "ETH": "ETHUSD", "SOL": "SOLUSD"}
	var r struct {
		Error  []string `json:"error"`
		Result map[string]struct {
			Bids [][]json.RawMessage `json:"bids"`
			Asks [][]json.RawMessage `json:"asks"`
		} `json:"result"`
	}
	// Kraken REST officially caps count at 500. This is only a fallback when
	// the short-lived depth=1000 WebSocket snapshot fails within the target window.
	u := "https://api.kraken.com/0/public/Depth?pair=" + url.QueryEscape(pairs[asset]) + "&count=500"
	recv, err := s.getJSON(ctx, u, &r)
	if err != nil {
		return nil, recv, err
	}
	if len(r.Error) > 0 {
		return nil, recv, fmt.Errorf("kraken=%s", strings.Join(r.Error, ","))
	}
	for _, b := range r.Result {
		return zonesFromLevels(parseRawLevels(b.Bids), parseRawLevels(b.Asks)), recv, nil
	}
	return nil, recv, fmt.Errorf("kraken_empty_result")
}

func krakenAsset(p string) string {
	switch p {
	case "BTC/USD":
		return "BTC"
	case "ETH/USD":
		return "ETH"
	case "SOL/USD":
		return "SOL"
	}
	return ""
}

func (s *SpotSampler) fetchKrakenBatch(ctx context.Context) []spotRESTResult {
	// Keep the exact depth=1000 WS snapshot for near-book precision. In
	// parallel, Kraken GroupedBook supplies a much wider aggregated book for
	// BTC/ETH. Exact values always win; grouped values only fill zone sides
	// that the exact 1000-level snapshot cannot cover.
	type groupedResult struct {
		asset  string
		zones  []CompactPair
		recvMs int64
		err    error
	}
	groupCh := make(chan groupedResult, 2)
	for _, asset := range []string{"BTC", "ETH"} {
		asset := asset
		go func() {
			z, recv, err := s.fetchKrakenGrouped(ctx, asset)
			groupCh <- groupedResult{asset: asset, zones: z, recvMs: recv, err: err}
		}()
	}

	exact := make(map[string]spotRESTResult, len(Assets))
	var wsErr error
	cn, _, err := websocket.DefaultDialer.DialContext(ctx, "wss://ws.kraken.com/v2", nil)
	if err == nil {
		defer cn.Close()
		if dl, ok := ctx.Deadline(); ok {
			_ = cn.SetReadDeadline(dl)
		}
		err = cn.WriteJSON(map[string]any{
			"method": "subscribe",
			"params": map[string]any{
				"channel":  "book",
				"symbol":   []string{"BTC/USD", "ETH/USD", "SOL/USD"},
				"depth":    1000,
				"snapshot": true,
			},
		})
	}
	if err != nil {
		wsErr = err
	} else {
		for len(exact) < len(Assets) {
			_, b, readErr := cn.ReadMessage()
			if readErr != nil {
				wsErr = readErr
				break
			}
			recv := nowMs()
			var m struct {
				Channel string `json:"channel"`
				Type    string `json:"type"`
				Data    []struct {
					Symbol string `json:"symbol"`
					Bids   []struct {
						Price float64 `json:"price"`
						Qty   float64 `json:"qty"`
					} `json:"bids"`
					Asks []struct {
						Price float64 `json:"price"`
						Qty   float64 `json:"qty"`
					} `json:"asks"`
				} `json:"data"`
			}
			if json.Unmarshal(b, &m) != nil || m.Channel != "book" || m.Type != "snapshot" {
				continue
			}
			for _, d := range m.Data {
				asset := krakenAsset(d.Symbol)
				if asset == "" {
					continue
				}
				bids := make([]Level, 0, len(d.Bids))
				for _, x := range d.Bids {
					if x.Price > 0 && x.Qty > 0 {
						bids = append(bids, Level{Price: x.Price, Qty: x.Qty})
					}
				}
				asks := make([]Level, 0, len(d.Asks))
				for _, x := range d.Asks {
					if x.Price > 0 && x.Qty > 0 {
						asks = append(asks, Level{Price: x.Price, Qty: x.Qty})
					}
				}
				exact[asset] = spotRESTResult{
					venue: "kraken", asset: asset, recvMs: recv,
					zones: zonesFromLevels(bids, asks),
				}
			}
		}
	}

	groupedByAsset := map[string]groupedResult{}
	for i := 0; i < 2; i++ {
		select {
		case g := <-groupCh:
			groupedByAsset[g.asset] = g
		case <-ctx.Done():
			i = 2
		}
	}

	out := make([]spotRESTResult, 0, len(Assets))
	for _, asset := range Assets {
		e, haveExact := exact[asset]
		g, haveGrouped := groupedByAsset[asset]

		if haveExact && haveGrouped && g.err == nil {
			e.zones = mergePreferExact(e.zones, g.zones)
			if g.recvMs > e.recvMs {
				e.recvMs = g.recvMs
			}
			out = append(out, e)
			continue
		}
		if haveExact {
			out = append(out, e)
			continue
		}
		if haveGrouped && g.err == nil {
			out = append(out, spotRESTResult{venue: "kraken", asset: asset, recvMs: g.recvMs, zones: g.zones})
			continue
		}

		// SOL normally fits fully inside the exact depth=1000 snapshot. If that
		// snapshot fails, retain the lightweight REST500 fallback rather than
		// issuing GroupedBook for SOL on every normal cycle.
		z, recv, restErr := s.fetchKrakenREST(ctx, asset)
		if restErr == nil {
			out = append(out, spotRESTResult{venue: "kraken", asset: asset, recvMs: recv, zones: z})
			continue
		}
		reason := restErr
		if haveGrouped && g.err != nil {
			reason = fmt.Errorf("ws=%v; grouped=%v; rest=%v", wsErr, g.err, restErr)
		} else if wsErr != nil {
			reason = fmt.Errorf("ws=%v; rest=%v", wsErr, restErr)
		}
		out = append(out, spotRESTResult{venue: "kraken", asset: asset, recvMs: recv, err: reason})
	}
	return out
}

func (s *SpotSampler) fetchBitfinexBook(ctx context.Context, asset, precision string) ([]CompactPair, int64, error) {
	symbols := map[string]string{"BTC": "tBTCUSD", "ETH": "tETHUSD", "SOL": "tSOLUSD"}
	var rows [][]float64
	u := fmt.Sprintf("https://api-pub.bitfinex.com/v2/book/%s/%s?len=250", url.PathEscape(symbols[asset]), precision)
	recv, err := s.getJSON(ctx, u, &rows)
	if err != nil {
		return nil, recv, err
	}
	bids := make([]Level, 0, len(rows)/2)
	asks := make([]Level, 0, len(rows)/2)
	for _, x := range rows {
		if len(x) < 3 || x[0] <= 0 || x[1] <= 0 || x[2] == 0 {
			continue
		}
		if x[2] > 0 {
			bids = append(bids, Level{Price: x[0], Qty: x[2]})
		} else {
			asks = append(asks, Level{Price: x[0], Qty: -x[2]})
		}
	}
	return zonesFromLevels(bids, asks), recv, nil
}

func (s *SpotSampler) fetchBitfinex(ctx context.Context, asset string) ([]CompactPair, int64, error) {
	type rr struct {
		z    []CompactPair
		recv int64
		err  error
	}
	p1Ch := make(chan rr, 1)
	p2Ch := make(chan rr, 1)
	go func() {
		z, recv, err := s.fetchBitfinexBook(ctx, asset, "P1")
		p1Ch <- rr{z: z, recv: recv, err: err}
	}()
	go func() {
		z, recv, err := s.fetchBitfinexBook(ctx, asset, "P2")
		p2Ch <- rr{z: z, recv: recv, err: err}
	}()

	var p1, p2 rr
	for i := 0; i < 2; i++ {
		select {
		case p1 = <-p1Ch:
			p1Ch = nil
		case p2 = <-p2Ch:
			p2Ch = nil
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}
	}

	// Fixed Bitfinex data regime:
	//   0-100 bps   -> P1
	//   100-750 bps -> P2
	// P0 is intentionally not requested so the near-zone time series never
	// switches precision from row to row.
	if p1.err != nil {
		return nil, p1.recv, fmt.Errorf("bitfinex_p1=%v", p1.err)
	}
	if p2.err != nil {
		return nil, p2.recv, fmt.Errorf("bitfinex_p2=%v", p2.err)
	}
	recv := p1.recv
	if p2.recv > recv {
		recv = p2.recv
	}
	return mergeBitfinexZones(p1.z, p2.z), recv, nil
}

func (s *SpotSampler) fetch(ctx context.Context, venue, asset string) ([]CompactPair, int64, error) {
	switch venue {
	case "binance":
		if asset != "SOL" {
			return nil, 0, fmt.Errorf("binance_local_book_asset=%s", asset)
		}
		return s.fetchBinanceSOL(ctx)
	case "coinbase":
		return s.fetchCoinbase(ctx, asset)
	case "bitfinex":
		return s.fetchBitfinex(ctx, asset)
	default:
		return nil, 0, fmt.Errorf("unknown_venue=%s", venue)
	}
}

func (s *SpotSampler) transition(target int64, venue, asset, status, reason string) {
	key := venue + ":" + asset
	s.mu.Lock()
	prev := s.last[key]
	if prev == status {
		s.mu.Unlock()
		return
	}
	s.last[key] = status
	s.mu.Unlock()
	if s.emit == nil {
		return
	}
	e := EventRecord{T: target, Venue: venue, Asset: asset, Reason: reason}
	switch status {
	case "OK":
		if prev == "" {
			return
		}
		e.Type = "snapshot_recovered"
		e.Recovered = true
	case "PARTIAL":
		e.Type = "coverage_loss"
		e.Code = "SPOT_PARTIAL"
	case "ERROR":
		e.Type = "api_error"
		e.Code = "SPOT_SOURCE"
	default:
		return
	}
	s.emit(e)
}

func (s *SpotSampler) Sample(ctx context.Context, targetMs int64) ([][][]CompactPair, int64) {
	d := emptySpotGrid()
	deadline := time.UnixMilli(targetMs + spotAcceptWindow.Milliseconds())
	batchCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	ch := make(chan spotRESTResult, len(Venues)*len(Assets))
	for _, venue := range Venues {
		if venue == "kraken" {
			continue
		}
		for _, asset := range Assets {
			// BTC/ETH use the persistent Binance local books below. SOL keeps
			// the 30-second REST snapshot path because 5000 levels
			// already cover the required 0-750 bps range.
			if venue == "binance" && asset != "SOL" {
				continue
			}
			venue, asset := venue, asset
			go func() {
				z, recv, err := s.fetch(batchCtx, venue, asset)
				ch <- spotRESTResult{venue: venue, asset: asset, recvMs: recv, zones: z, err: err}
			}()
		}
	}
	go func() {
		// REST/snapshot sources are allowed to start at T-1.5s. The persistent
		// Binance books must instead be cut exactly at decision time T so the
		// row does not silently lag the other venues by about one second.
		if !sleepUntil(batchCtx, time.UnixMilli(targetMs)) {
			return
		}
		for _, asset := range []string{"BTC", "ETH"} {
			src := s.sources[sourceKey("binance", asset)]
			if src == nil {
				ch <- spotRESTResult{venue: "binance", asset: asset, recvMs: targetMs, err: fmt.Errorf("binance_local_source_missing")}
				continue
			}
			z := src.SnapshotZonesAdvance(targetMs, targetMs+s.cadence.Milliseconds(), 45_000)
			ch <- spotRESTResult{venue: "binance", asset: asset, recvMs: targetMs, zones: z}
		}
	}()
	go func() {
		for _, r := range s.fetchKrakenBatch(batchCtx) {
			ch <- r
		}
	}()

	vi := map[string]int{}
	ai := map[string]int{}
	for i, v := range Venues {
		vi[v] = i
	}
	for i, a := range Assets {
		ai[a] = i
	}
	seen := map[string]bool{}
	maxRecv := int64(0)
	want := len(Venues) * len(Assets)
	for len(seen) < want {
		select {
		case r := <-ch:
			key := r.venue + ":" + r.asset
			if seen[key] {
				continue
			}
			seen[key] = true
			if r.err != nil {
				s.transition(targetMs, r.venue, r.asset, "ERROR", r.err.Error())
				continue
			}
			if !withinSpotWindow(targetMs, r.recvMs) {
				s.transition(targetMs, r.venue, r.asset, "ERROR", fmt.Sprintf("recv_outside_window recv=%d target=%d", r.recvMs, targetMs))
				continue
			}
			if r.recvMs > maxRecv {
				maxRecv = r.recvMs
			}
			d[vi[r.venue]][ai[r.asset]] = r.zones
			if completeZones(r.zones) {
				s.transition(targetMs, r.venue, r.asset, "OK", "")
			} else {
				s.transition(targetMs, r.venue, r.asset, "PARTIAL", "one_or_more_zone_sides_null")
			}
		case <-batchCtx.Done():
			for _, venue := range Venues {
				for _, asset := range Assets {
					key := venue + ":" + asset
					if !seen[key] {
						s.transition(targetMs, venue, asset, "ERROR", "deadline")
						seen[key] = true
					}
				}
			}
		}
	}
	if maxRecv == 0 {
		maxRecv = nowMs()
	}
	return d, maxRecv
}
