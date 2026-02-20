# Monitoring API

Management contracts are versioned under `/api/v1` and defined in the
`monitoring-rest-json-api` module (`org.hestiastore.monitoring.json.api.*`).

This page describes the target `v1` shape after runtime override support for
selected cache-related properties is applied.

## Versioning policy

- Current version: `v1`.
- Path prefix: `/api/v1`.
- Backward compatibility:
  - Existing field names and endpoint paths stay stable within `v1`.
  - New optional fields may be added.
  - Unknown fields must be ignored by clients.
- Deprecation:
  - Endpoints/fields are first marked as deprecated in docs and changelog.
  - Removal requires introducing a newer API version path (for example
    `/api/v2`).

## Endpoints

- `GET /api/v1/report`
- `POST /api/v1/actions/flush`
- `POST /api/v1/actions/compact`
- `GET /api/v1/config?indexName=<required>`
- `PATCH /api/v1/config?indexName=<required>`

## Runtime-overridable keys

- `maxNumberOfSegmentsInCache`
- `maxNumberOfKeysInSegmentCache`
- `maxNumberOfKeysInSegmentWriteCache`
- `maxNumberOfKeysInSegmentWriteCacheDuringMaintenance`

These overrides are runtime-only:

- applied in-memory for the running JVM
- not persisted to index metadata (`meta.txt` / configuration property store)
- reset on process restart

## Example payloads

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
      "maxNumberOfKeysInSegmentWriteCache": 120000,
      "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance": 180000,
      "segmentCount": 24,
      "segmentReadyCount": 24,
      "segmentMaintenanceCount": 0,
      "segmentErrorCount": 0,
      "segmentClosedCount": 0,
      "segmentBusyCount": 0,
      "totalSegmentKeys": 1489200,
      "totalSegmentCacheKeys": 402100,
      "totalWriteCacheKeys": 8700,
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

- `original` and `current` contain full index configuration values (including
  values that are **not** runtime-changeable).
- `supportedKeys` is the exact writable subset for `PATCH /api/v1/config`.
- Any key present in `original/current` but missing from `supportedKeys` is
  read-only at runtime.

```json
{
  "indexName": "orders",
  "original": {
    "maxNumberOfSegmentsInCache": 128,
    "maxNumberOfKeysInSegment": 500000,
    "maxNumberOfKeysInSegmentChunk": 1024,
    "maxNumberOfKeysInSegmentCache": 200000,
    "maxNumberOfKeysInSegmentWriteCache": 100000,
    "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance": 140000,
    "maxNumberOfDeltaCacheFiles": 128,
    "bloomFilterNumberOfHashFunctions": 7,
    "bloomFilterIndexSizeInBytes": 262144,
    "indexWorkerThreadCount": 8,
    "numberOfIoThreads": 8,
    "numberOfSegmentIndexMaintenanceThreads": 6,
    "numberOfIndexMaintenanceThreads": 4,
    "numberOfRegistryLifecycleThreads": 2,
    "indexBusyBackoffMillis": 10,
    "indexBusyTimeoutMillis": 5000,
    "diskIoBufferSize": 4096
  },
  "current": {
    "maxNumberOfSegmentsInCache": 256,
    "maxNumberOfKeysInSegment": 500000,
    "maxNumberOfKeysInSegmentChunk": 1024,
    "maxNumberOfKeysInSegmentCache": 260000,
    "maxNumberOfKeysInSegmentWriteCache": 120000,
    "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance": 180000,
    "maxNumberOfDeltaCacheFiles": 128,
    "bloomFilterNumberOfHashFunctions": 7,
    "bloomFilterIndexSizeInBytes": 262144,
    "indexWorkerThreadCount": 8,
    "numberOfIoThreads": 8,
    "numberOfSegmentIndexMaintenanceThreads": 6,
    "numberOfIndexMaintenanceThreads": 4,
    "numberOfRegistryLifecycleThreads": 2,
    "indexBusyBackoffMillis": 10,
    "indexBusyTimeoutMillis": 5000,
    "diskIoBufferSize": 4096
  },
  "supportedKeys": [
    "maxNumberOfKeysInSegmentCache",
    "maxNumberOfKeysInSegmentWriteCache",
    "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance",
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
    "maxNumberOfKeysInSegmentWriteCache": "120000",
    "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance": "180000"
  },
  "dryRun": false
}
```

`PATCH /api/v1/config?indexName=orders` response:

- `204 No Content` on success.

## Validation rules

- `indexName` query parameter is required for config read/update.
- PATCH accepts only keys listed in `supportedKeys` returned by `GET /config`.
- Keys present in `original/current` but not listed in `supportedKeys` are
  read-only and cannot be patched.
- `maxNumberOfSegmentsInCache >= 3`
- `maxNumberOfKeysInSegmentCache >= 1`
- `maxNumberOfKeysInSegmentWriteCache >= 1`
- `maxNumberOfKeysInSegmentWriteCacheDuringMaintenance > maxNumberOfKeysInSegmentWriteCache`
- Unknown key error code: `CONFIG_KEY_NOT_SUPPORTED`

## Error response

```json
{
  "code": "INVALID_STATE",
  "message": "Operation is not allowed.",
  "requestId": "req-20260220-001",
  "capturedAt": "2026-02-20T18:00:02Z"
}
```
