# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `1756ecbfaee92319c50abf1110e55f35b5e37fd0`
- Candidate SHA: `b9d7389a0e02d0ebf42fe1df6912f5ac7fb1b572`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1861822.373 ops/s` | `1934703.232 ops/s` | `+3.91%` | `better` |
| `segment-index-get-live:getMissSync` | `1800476.804 ops/s` | `1810677.854 ops/s` | `+0.57%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1712336.970 ops/s` | `1751522.403 ops/s` | `+2.29%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1817303.978 ops/s` | `1794491.745 ops/s` | `-1.26%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1853289.730 ops/s` | `1849467.190 ops/s` | `-0.21%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1103966.452 ops/s` | `1095971.547 ops/s` | `-0.72%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `282871.743 ops/s` | `262914.171 ops/s` | `-7.06%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `134294.221 ops/s` | `122893.736 ops/s` | `-8.49%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `148577.522 ops/s` | `140020.435 ops/s` | `-5.76%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `166562.236 ops/s` | `157288.153 ops/s` | `-5.57%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `151541.612 ops/s` | `142637.710 ops/s` | `-5.88%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15020.624 ops/s` | `14650.443 ops/s` | `-2.46%` | `neutral` |
