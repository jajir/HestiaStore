# Monitoring Bridge

HestiaStore monitoring is split into dedicated modules so the core `index`
artifact has no runtime dependency on Micrometer or Prometheus.

## Modules

- `index`  
  Core storage/index implementation and metrics snapshot API.
- `monitoring-api`  
  Shared metric names/contracts for monitoring integrations.
- `monitoring-micrometer`  
  Micrometer binder over `SegmentIndex.metricsSnapshot()`.
- `monitoring-prometheus`  
  Prometheus helper based on Micrometer Prometheus registry.

## Runtime model

- Core publishes immutable snapshots via `metricsSnapshot()`.
- Monitoring modules are optional and attached by the embedding application.
- If monitoring modules are not on classpath, core behavior is unchanged.

## Exposed metric names

- `hestiastore_ops_get_total`
- `hestiastore_ops_put_total`
- `hestiastore_ops_delete_total`
- `hestiastore_index_up`

All counters use the stable `index` tag to identify the logical index name.
