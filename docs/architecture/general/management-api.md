# Management API

Management contracts are versioned under `/api/v1` and defined in the
`management-api` module (`org.hestiastore.management.api.*`).

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
- `PATCH /api/v1/config`

## Example payloads

`GET /api/v1/report` response:

```json
{
  "jvm": {
    "heapUsedBytes": 104857600,
    "heapCommittedBytes": 268435456,
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
      "deleteOperationCount": 120
    },
    {
      "indexName": "customers",
      "state": "READY",
      "ready": true,
      "getOperationCount": 501,
      "putOperationCount": 77,
      "deleteOperationCount": 3
    }
  ],
  "capturedAt": "2026-02-17T18:00:00Z"
}
```

`POST /api/v1/actions/flush` request:

```json
{
  "requestId": "req-20260217-001",
  "indexName": "orders"
}
```

`indexName` is optional. If omitted, action is applied to all indexes currently
registered in `ManagementAgentServer`.

`POST /api/v1/actions/flush` response:

```json
{
  "requestId": "req-20260217-001",
  "action": "FLUSH",
  "status": "COMPLETED",
  "message": "",
  "capturedAt": "2026-02-17T18:00:01Z"
}
```

`PATCH /api/v1/config` request:

```json
{
  "values": {
    "indexBusyTimeoutMillis": "2500"
  },
  "dryRun": true
}
```

Error response:

```json
{
  "code": "INVALID_STATE",
  "message": "Operation is not allowed.",
  "requestId": "req-20260217-001",
  "capturedAt": "2026-02-17T18:00:02Z"
}
```
