# IMPORTANT UNIT UPDATE

All Spot depth values in the current Production contract are **USD notional**, not base-asset quantity.

```text
level USD notional = price × base quantity
quote currency => treated as USD 1:1
no FX conversion
```

Legacy base-quantity data is intentionally deleted during the one-time USD dataset migration.

# Historical pagination v1.1 examples

## First page

Request:

```http
GET /api/v1/history/depth/index?limit=2&cursor=0 HTTP/1.1
Host: marketdepth-production.up.railway.app
Authorization: Bearer <HISTORY_READ_TOKEN>
```

Example response:

```json
{
  "ok": true,
  "data": [
    {
      "kind": "spot",
      "id": "spot-1790347500000",
      "rel": "chunks/spot/1790347500000.jsonl.gz",
      "shaRel": "chunks/spot/1790347500000.jsonl.gz.sha256",
      "startTsMs": 1790347500000,
      "endTsMs": 1790348400000,
      "rows": 30,
      "bytes": 12345,
      "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      "schema": "MD-SPOT-30S-USD-2"
    },
    {
      "kind": "spot",
      "id": "spot-1790348400000",
      "rel": "chunks/spot/1790348400000.jsonl.gz",
      "shaRel": "chunks/spot/1790348400000.jsonl.gz.sha256",
      "startTsMs": 1790348400000,
      "endTsMs": 1790349300000,
      "rows": 30,
      "bytes": 12411,
      "sha256": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
      "schema": "MD-SPOT-30S-USD-2"
    }
  ],
  "nextCursor": 2,
  "meta": {
    "schema": "MD-HISTORY-INDEX-2",
    "depthUnit": "USD_NOTIONAL",
    "quoteToUSDPolicy": "RAW_QUOTE_PRICE_ASSUMED_USD_1_TO_1_NO_FX",
    "timezone": "Asia/Taipei",
    "utcOffset": "+08:00",
    "indexVersion": 1790350000123,
    "indexUpdatedTsMs": 1790350000123
  }
}
```

Rules:

- On page 1, omit `indexVersion`.
- Save `meta.indexVersion`.
- `meta.indexUpdatedTsMs` is informational and currently equals `meta.indexVersion`.

## Second page

Request:

```http
GET /api/v1/history/depth/index?limit=2&cursor=2&indexVersion=1790350000123 HTTP/1.1
Host: marketdepth-production.up.railway.app
Authorization: Bearer <HISTORY_READ_TOKEN>
```

Use the query parameter exactly named:

```text
indexVersion
```

Do not use a custom header.

Example success:

```json
{
  "ok": true,
  "data": [
    {
      "kind": "spot",
      "id": "spot-1790349300000",
      "rel": "chunks/spot/1790349300000.jsonl.gz",
      "shaRel": "chunks/spot/1790349300000.jsonl.gz.sha256",
      "startTsMs": 1790349300000,
      "endTsMs": 1790350200000,
      "rows": 30,
      "bytes": 12480,
      "sha256": "1111111111111111111111111111111111111111111111111111111111111111",
      "schema": "MD-SPOT-30S-USD-2"
    }
  ],
  "nextCursor": -1,
  "meta": {
    "schema": "MD-HISTORY-INDEX-2",
    "timezone": "Asia/Taipei",
    "utcOffset": "+08:00",
    "indexVersion": 1790350000123,
    "indexUpdatedTsMs": 1790350000123
  }
}
```

Every page in one pagination round must use the same first-page `indexVersion`.

## If index changes between pages

This can happen because:

- a new 15-minute chunk is sealed, or
- admin purges previously downloaded historical data.

Request made with the old version:

```http
GET /api/v1/history/depth/index?limit=2&cursor=2&indexVersion=1790350000123
```

Response:

```http
HTTP/1.1 409 Conflict
Content-Type: application/json
```

```json
{
  "ok": false,
  "error": {
    "code": "INDEX_CHANGED",
    "message": "Historical index changed during pagination; restart from the first page"
  },
  "meta": {
    "requestedIndexVersion": 1790350000123,
    "currentIndexVersion": 1790350900456,
    "indexUpdatedTsMs": 1790350900456
  }
}
```

Frontend action:

1. Discard the unfinished in-memory index listing for that pagination round.
2. Restart at `cursor=0` with no `indexVersion`.
3. Pin the newly returned `meta.indexVersion` for the new round.
4. Keep already downloaded chunk files whose SHA256 has already been verified; they are immutable and do not need to be downloaded again merely because the index version changed.
5. If a cached chunk is no longer present in the new index after purge, it may remain in local cache according to local cache policy, but must not be treated as currently retained server history.

## Legacy clients

A client that never sends `indexVersion` remains accepted:

```http
GET /api/v1/history/depth/index?limit=100&cursor=100
```

The server returns 200 and current index data using the existing cursor behavior.

Compatibility consequence:

- no breaking change for old clients;
- however, old clients do not get cross-page snapshot consistency;
- if the index changes during their pagination, an old client can still theoretically skip or repeat entries.

New frontend/BFF must use `indexVersion`.
