# Monitoring API

Management contracts are versioned under `/api/v1` and defined in the
`monitoring-rest-json-api` module (`org.hestiastore.monitoring.json.api.*`).

The current runtime model is direct-to-segment. The REST contract uses the same
canonical write-path names as Java configuration and runtime tuning.

## Versioning Policy

- Current version: `v1`.
- Path prefix: `/api/v1`.
- Unknown fields must be ignored by clients.
- API-breaking changes require a newer API version path.

## Endpoints

- `GET /api/v1/report`
- `POST /api/v1/actions/flush`
- `POST /api/v1/actions/compact`
- `GET /api/v1/config?indexName=<required>`
- `PATCH /api/v1/config?indexName=<required>`

## Runtime-Overridable Keys

- `maxNumberOfSegmentsInCache`
- `maxNumberOfKeysInSegmentCache`
- `segmentWriteCacheKeyLimit`
- `segmentWriteCacheKeyLimitDuringMaintenance`
- `indexBufferedWriteKeyLimit`
- `segmentSplitKeyThreshold`

These overrides are runtime-only:

- applied in-memory for the running JVM
- not persisted to index metadata
- reset on process restart

## Example Payloads

`GET /api/v1/report` response:

```json
{
  "jvm": {
    "heapUsedBytes": 104857600,
    "heapCommittedBytes": 268435456,
    "heapMaxBytes": 536870912,
    "nonHeapUsedBytes": 52428800,
    "gcCount": 42,
    "gcTimeMillis": 917
  },
  "indexes": [
    {
      "indexName": "orders",
      "state": "READY",
      "ready": true,
      "getOperationCount": 12345,
      "putOperationCount": 6789,
      "deleteOperationCount": 120,
      "registryCacheHitCount": 25000,
      "registryCacheMissCount": 320,
      "registryCacheLoadCount": 320,
      "registryCacheEvictionCount": 18,
      "registryCacheSize": 96,
      "registryCacheLimit": 128,
      "segmentCacheKeyLimitPerSegment": 260000,
      "segmentWriteCacheKeyLimit": 120000,
      "segmentWriteCacheKeyLimitDuringMaintenance": 180000,
      "indexBufferedWriteKeyLimit": 720000,
      "segmentCount": 24,
      "segmentReadyCount": 24,
      "segmentMaintenanceCount": 0,
      "segmentErrorCount": 0,
      "segmentClosedCount": 0,
      "segmentBusyCount": 0,
      "totalSegmentKeys": 1489200,
      "totalSegmentCacheKeys": 402100,
      "totalBufferedWriteKeys": 8700,
      "totalDeltaCacheFiles": 31,
      "compactRequestCount": 17,
      "flushRequestCount": 42,
      "splitScheduleCount": 9,
      "splitInFlightCount": 0,
      "maintenanceQueueSize": 0,
      "maintenanceQueueCapacity": 1024,
      "splitQueueSize": 0,
      "splitQueueCapacity": 256,
      "readLatencyP50Micros": 78,
      "readLatencyP95Micros": 240,
      "readLatencyP99Micros": 710,
      "writeLatencyP50Micros": 110,
      "writeLatencyP95Micros": 350,
      "writeLatencyP99Micros": 980,
      "bloomFilterHashFunctions": 7,
      "bloomFilterIndexSizeInBytes": 262144,
      "bloomFilterProbabilityOfFalsePositive": 0.01,
      "bloomFilterRequestCount": 650000,
      "bloomFilterRefusedCount": 402000,
      "bloomFilterPositiveCount": 248000,
      "bloomFilterFalsePositiveCount": 2100
    }
  ],
  "capturedAt": "2026-02-20T18:00:00Z"
}
```

`GET /api/v1/config?indexName=orders` response:

```json
{
  "indexName": "orders",
  "original": {
    "maxNumberOfSegmentsInCache": 128,
    "maxNumberOfKeysInSegmentCache": 200000,
    "segmentWriteCacheKeyLimit": 100000,
    "segmentWriteCacheKeyLimitDuringMaintenance": 140000,
    "indexBufferedWriteKeyLimit": 560000,
    "segmentSplitKeyThreshold": 500000
  },
  "current": {
    "maxNumberOfSegmentsInCache": 256,
    "maxNumberOfKeysInSegmentCache": 260000,
    "segmentWriteCacheKeyLimit": 120000,
    "segmentWriteCacheKeyLimitDuringMaintenance": 180000,
    "indexBufferedWriteKeyLimit": 720000,
    "segmentSplitKeyThreshold": 500000
  },
  "supportedKeys": [
    "maxNumberOfKeysInSegmentCache",
    "segmentWriteCacheKeyLimit",
    "segmentWriteCacheKeyLimitDuringMaintenance",
    "indexBufferedWriteKeyLimit",
    "segmentSplitKeyThreshold",
    "maxNumberOfSegmentsInCache"
  ],
  "revision": 12,
  "capturedAt": "2026-02-20T18:00:01Z"
}
```

`PATCH /api/v1/config?indexName=orders` request:

```json
{
  "values": {
    "maxNumberOfSegmentsInCache": "256",
    "maxNumberOfKeysInSegmentCache": "260000",
    "segmentWriteCacheKeyLimit": "120000",
    "segmentWriteCacheKeyLimitDuringMaintenance": "180000",
    "indexBufferedWriteKeyLimit": "720000",
    "segmentSplitKeyThreshold": "500000"
  },
  "dryRun": false
}
```

`PATCH /api/v1/config?indexName=orders` returns `204 No Content` on success.

## Validation Rules

- `indexName` query parameter is required for config read/update.
- `GET /config` exposes only runtime-tunable keys.
- PATCH accepts only keys listed in `supportedKeys`.
- `maxNumberOfSegmentsInCache >= 3`
- `maxNumberOfKeysInSegmentCache >= 1`
- `segmentWriteCacheKeyLimit >= 1`
- `segmentWriteCacheKeyLimitDuringMaintenance > segmentWriteCacheKeyLimit`
- `indexBufferedWriteKeyLimit >= segmentWriteCacheKeyLimitDuringMaintenance`
- `segmentSplitKeyThreshold >= segmentWriteCacheKeyLimitDuringMaintenance`
- Unknown key error code: `CONFIG_KEY_NOT_SUPPORTED`
