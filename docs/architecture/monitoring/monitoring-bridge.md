# Monitoring Bridge

HestiaStore monitoring is split into dedicated modules so the core `engine`
artifact has no runtime dependency on Micrometer or Prometheus.

## Modules

- `engine`
  Core storage/index implementation and runtime snapshot API.
- `monitoring-rest-json-api`
  Shared REST/JSON DTO contracts for monitoring and management.
- `monitoring-micrometer`
  Micrometer binder over `SegmentIndex.runtimeMonitoring().snapshot().getMetrics()`.
- `monitoring-prometheus`
  Prometheus helper based on Micrometer Prometheus registry.
- `monitoring-rest-json`
  Node-local REST bridge exposing `/api/v1/*` for report/actions/config.
- `monitoring-console-web`
  Operator UI that polls one or more node-local `monitoring-rest-json` endpoints.

## How runtime data is produced

At runtime, each logical index exposes counters and state via
`SegmentIndex.runtimeMonitoring().snapshot().getMetrics()`.

The snapshot includes:

- operation counters (`get/put/delete`)
- segment registry cache counters (hit/miss/load/evict, size, limit)
- current configuration-derived runtime limits shown in monitoring
- segment runtime state counts and queue pressure
- latency percentiles and bloom-filter counters

Because snapshots are immutable, readers (API/exporters/UI) can observe metrics
without mutating index state.

## Runtime model

- Core publishes immutable snapshots via `metricsSnapshot()`.
- Monitoring modules are optional and attached by the embedding application.
- If monitoring modules are not on classpath, core behavior is unchanged.

## Exposed metric names

- `hestiastore_ops_get_total`
- `hestiastore_ops_put_total`
- `hestiastore_ops_delete_total`
- `hestiastore_partition_count`
- `hestiastore_partition_buffered_key_count`
- `hestiastore_partition_drain_schedule_total`
- `hestiastore_partition_drain_in_flight`
- `hestiastore_partition_drain_latency_p95_micros`
- `hestiastore_split_schedule_total`
- `hestiastore_split_in_flight`
- `hestiastore_index_up`

All counters use the stable `index` tag to identify the logical index name.
