# Monitoring API

Management contracts are versioned under `/api/v1` and defined in the
`monitoring-rest-json-api` module (`org.hestiastore.monitoring.json.api.*`).

This page describes the target `v1` shape after runtime override support for
selected partition/cache runtime limits is applied.

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
- `maxNumberOfKeysInActivePartition`
- `maxNumberOfImmutableRunsPerPartition`
- `maxNumberOfKeysInPartitionBuffer`
- `maxNumberOfKeysInIndexBuffer`
- `maxNumberOfKeysInPartitionBeforeSplit`

These overrides are runtime-only:

- applied in-memory for the running JVM
- not persisted to index metadata (`manifest.txt` / configuration property
  store)
- reset on process restart

Compatibility note:

- report and config responses emit only the canonical partition-aware names
- `supportedKeys` and config views return only the canonical partition-aware
  names

Lifecycle note:

- `state` is one of `OPENING`, `READY`, `CLOSING`, `CLOSED`, or `ERROR`.
- `ready` is `true` only when `state == "READY"`.
- `CLOSING` means shutdown is in progress and final maintenance/persistence
  work has not finished yet.

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
      "maxNumberOfKeysInActivePartition": 120000,
      "maxNumberOfImmutableRunsPerPartition": 2,
      "maxNumberOfKeysInPartitionBuffer": 180000,
      "maxNumberOfKeysInIndexBuffer": 720000,
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
      "partitionCount": 24,
      "activePartitionCount": 3,
      "drainingPartitionCount": 1,
      "immutableRunCount": 2,
      "partitionBufferedKeyCount": 8700,
      "localThrottleCount": 0,
      "globalThrottleCount": 0,
      "drainScheduleCount": 9,
      "drainInFlightCount": 0,
      "drainLatencyP95Micros": 420,
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

- `original` and `current` contain only the canonical runtime-tuning view used
  by `PATCH /api/v1/config`.
- Read-only or non-runtime properties are intentionally omitted from this
  endpoint even if they exist in the persisted index manifest.
- `supportedKeys` is the exact writable subset for `PATCH /api/v1/config` and
  matches the key domain of `original/current`.

```json
{
  "indexName": "orders",
  "original": {
    "maxNumberOfSegmentsInCache": 128,
    "maxNumberOfKeysInSegmentCache": 200000,
    "maxNumberOfKeysInActivePartition": 100000,
    "maxNumberOfImmutableRunsPerPartition": 2,
    "maxNumberOfKeysInPartitionBuffer": 140000,
    "maxNumberOfKeysInIndexBuffer": 560000,
    "maxNumberOfKeysInPartitionBeforeSplit": 500000
  },
  "current": {
    "maxNumberOfSegmentsInCache": 256,
    "maxNumberOfKeysInSegmentCache": 260000,
    "maxNumberOfKeysInActivePartition": 120000,
    "maxNumberOfImmutableRunsPerPartition": 2,
    "maxNumberOfKeysInPartitionBuffer": 180000,
    "maxNumberOfKeysInIndexBuffer": 720000,
    "maxNumberOfKeysInPartitionBeforeSplit": 500000
  },
  "supportedKeys": [
    "maxNumberOfKeysInSegmentCache",
    "maxNumberOfKeysInActivePartition",
    "maxNumberOfImmutableRunsPerPartition",
    "maxNumberOfKeysInPartitionBuffer",
    "maxNumberOfKeysInIndexBuffer",
    "maxNumberOfKeysInPartitionBeforeSplit",
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
    "maxNumberOfKeysInActivePartition": "120000",
    "maxNumberOfImmutableRunsPerPartition": "2",
    "maxNumberOfKeysInPartitionBuffer": "180000",
    "maxNumberOfKeysInIndexBuffer": "720000",
    "maxNumberOfKeysInPartitionBeforeSplit": "500000"
  },
  "dryRun": false
}
```

`PATCH /api/v1/config?indexName=orders` response:

- `204 No Content` on success.

## Validation rules

- `indexName` query parameter is required for config read/update.
- `GET /config` exposes only runtime-tunable keys; persisted read-only settings
  are out of scope for this endpoint.
- PATCH accepts only keys listed in `supportedKeys` returned by `GET /config`.
- `maxNumberOfSegmentsInCache >= 3`
- `maxNumberOfKeysInSegmentCache >= 1`
- `maxNumberOfKeysInActivePartition >= 1`
- `maxNumberOfImmutableRunsPerPartition >= 1`
- `maxNumberOfKeysInPartitionBuffer > maxNumberOfKeysInActivePartition`
- `maxNumberOfKeysInIndexBuffer >= maxNumberOfKeysInPartitionBuffer`
- `maxNumberOfKeysInPartitionBeforeSplit >= maxNumberOfKeysInPartitionBuffer`
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
