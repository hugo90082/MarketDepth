# MarketDepth

Multi-exchange market-depth collector for BTC, ETH and SOL.

MarketDepth collects synchronized Spot depth snapshots from Binance, Coinbase, Kraken and Bitfinex, keeps 1-second Binance USDⓈ-M futures prices, stores immutable historical chunks, and exposes authenticated Historical + Recent APIs for a frontend/BFF.

## Frozen Spot specification

- Assets: `BTC`, `ETH`, `SOL`
- Venues: `binance`, `coinbase`, `kraken`, `bitfinex`
- Spot decision cadence: **30 seconds**
- Futures cadence: **1 second**
- Display timezone: **Asia/Taipei (+08:00)**
- Stored depth zones:
  - `0 <= d < 100 bps`
  - `100 <= d < 200 bps`
  - `200 <= d < 300 bps`
  - `300 <= d < 500 bps`
  - `500 <= d < 750 bps`

Each Spot row uses:

```text
d[venue][asset][zone] = [bidUSDNotional, askUSDNotional]
```

Values are **USD notional**.

Each order-book level contributes:

```text
USD notional = price × base-asset quantity
```

No FX conversion is applied to the quote currency. Current USD/USDT markets, and any future quote currency used by this collector, are treated as USD 1:1 by policy.

## Missing-data semantics

Coverage is evaluated independently for every:

```text
venue × asset × zone × side
```

A missing outer zone must **not** invalidate inner zones.

Examples:

```text
0–100     [120.0, 115.0]
100–200   [85.0, 93.0]
200–300   [44.0, 51.0]
300–500   [null, 37.0]
500–750   [null, null]
```

Meaning:

- `0` = coverage is confirmed for that whole zone/side and the actual summed USD notional is zero.
- `null` = the collector cannot confirm complete coverage for that zone/side, or that source failed.
- Bid and Ask validity are independent.
- A source failure affects only that venue/asset. It does not blank other venues/assets.
- Missing values are never forward-filled, backfilled, interpolated, or converted to zero.
- A scheduler miss still writes the 30-second target row as all-null so the time grid remains explicit.

## Historical storage

- Historical Spot chunks: **15 minutes**
- Expected complete Spot rows per chunk: **30**
- Chunk format: immutable gzip JSONL
- SHA256 + ETag supported
- Open buffers have crash-safe recovery checkpoints
- Package interval: 12 hours by default
- Disk-pressure protection retained

## Recent API

Recent Spot rows are kept in a **16-minute** in-memory buffer and rebuilt from sealed/open storage after restart.

```http
GET /api/v1/recent/depth
GET /api/v1/recent/depth?since=<targetTsMs>
```

Recent no longer has a dedicated 30-second cooldown.

All authenticated data APIs now share a process-wide rolling rate limit:

```text
maximum 2 accepted requests per rolling 1 second
```

Requests beyond that limit return HTTP `429` / `RATE_LIMIT` with `Retry-After: 1`.

This applies to Recent, Historical index, and Historical chunk downloads so the frontend BFF can hydrate history much faster than the former 30/min + 30-second Recent throttles while still keeping a hard server-side ceiling.

Historical and Recent use the same Spot row schema. Frontends merge on `t = targetTsMs`, with sealed Historical rows authoritative on overlap.

## Historical API

```http
GET /api/v1/history/depth/index
GET /api/v1/history/depth/chunks/{chunkId}
```

All data APIs use:

```http
Authorization: Bearer <HISTORY_READ_TOKEN>
```

The token is intended to remain server-side behind the frontend BFF; do not expose it in browser JavaScript.

## Spot acquisition

The proven transport approach is retained while the decision cadence changes to 30 seconds:

- Binance BTC/ETH: persistent local book, REST5000 bootstrap + diff-depth WebSocket + sequence validation + exact-target causal cut. Zone-side validity is authorized only by the latest complete REST5000 bootstrap trusted bid/ask edges. During the current RAM experiment, all sequence-valid diff levels are retained as observed-only local-book state, including levels outside trusted REST edges. Those outer levels never authorize zone validity. Zero-quantity updates still delete cancelled/removed levels. A normal rebootstrap refreshes the newly proven REST interval without discarding sequence-maintained outer observations; a true disconnect or sequence gap rebuilds from a clean snapshot.
- Binance SOL: REST5000 snapshot.
- Coinbase: full Level-2 REST snapshot.
- Kraken: exact WebSocket depth=1000; BTC/ETH also use GroupedBook only to fill uncovered sides/zones; REST fallback remains.
- Bitfinex: P1 is the fixed source for 0–100 bps and P2 is the fixed source for 100–750 bps. P0 is not requested. This keeps the near-zone precision regime constant across rows while preserving the existing outer-zone P2 regime. The stored schema and zone boundaries do not change. This fixed P1/P2 mapping is the production acquisition policy.

No future data are used for a target row.

## Required environment variables

```text
ADMIN_PASSWORD
SESSION_SECRET
HISTORY_READ_TOKEN
```

Optional storage/resource variables keep safe defaults:

```text
DATA_DIR
DATASET_DIR
PACKAGE_DURATION_MS
PACKAGE_MAX_BYTES
NOTICE_FREE_BYTES
WARNING_FREE_BYTES
URGENT_FREE_BYTES
PROTECT_FREE_BYTES
STOP_FREE_BYTES
```

Default dataset path:

```text
/data/market-depth
```

## Railway

The repository root is directly deployable. No subdirectory/root-directory override is required.

```text
Dockerfile
railway.toml
```

The Docker build runs:

```text
go test ./...
```

before producing the runtime binary.


## USD-notional dataset migration

The USD-notional format is intentionally incompatible with the previous base-quantity dataset.

On the first startup with:

```text
DatasetFormat = MARKET_DEPTH_USD_NOTIONAL_TRUSTED_COVERAGE_V2
```

MarketDepth deletes the legacy dataset under `DATASET_DIR`, writes a format marker, and starts a clean dataset. Subsequent restarts with the same marker preserve the new data.

Current schemas:

```text
MARKET_DEPTH_COLLECTION_30S_5ZONE_USD_3
MD-SPOT-30S-USD-3
MD-HISTORY-INDEX-3
MD-PACKAGE-3
```


## Binance trusted-coverage rule

For Binance BTC/ETH, the local order book can contain price levels learned later from WebSocket diff-depth updates beyond the original REST5000 bootstrap range. Those isolated outer levels do **not** prove that every untouched price level in between was known.

Therefore:

```text
zone validity = latest complete REST5000 bootstrap trusted edge coverage
outer diff level = book update only, never new coverage evidence
```

Bid and Ask are evaluated independently. If a zone side exceeds the trusted bootstrap edge, that side is stored as `null`, even when farther observed diff levels exist.


## Binance local-book memory bound

The BTC/ETH persistent Binance books are intentionally bounded to the latest REST5000 trusted absolute price interval. To prevent a fixed absolute interval from becoming stale as price moves, the collector automatically re-bootstraps when either trusted side approaches an adaptive headroom floor and also at least every 15 minutes:

```text
bid: keep price >= trustedBidEdge
ask: keep price <= trustedAskEdge
```

Current experiment: WebSocket diff messages outside the latest trusted REST interval are retained in memory as observed-only levels to measure real RAM growth when the collector does not prune outer prices. They still do not expand trusted coverage. Qty=0 updates continue to remove levels because retaining cancelled orders would make the book invalid and would measure artificial garbage rather than real active-book memory. Periodic/adaptive REST5000 refreshes replace only the REST-proven absolute interval while keeping sequence-maintained outer observations. A disconnect or sequence gap invalidates that continuity and therefore resets to a clean snapshot. Reconnect/bootstrap failures use exponential backoff (up to 30 seconds) instead of hammering REST.

Runtime status logs include:

```text
heapAllocBytes
heapInuseBytes
heapSysBytes
stackInuseBytes
numGC
```

This optimization changes only in-memory retention and observability. It does not change the Spot schema, Historical data contract, or trusted-coverage validity semantics.
