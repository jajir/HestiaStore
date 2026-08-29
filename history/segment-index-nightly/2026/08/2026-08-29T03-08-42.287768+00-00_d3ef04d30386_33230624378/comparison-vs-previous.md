# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4612407.089 ops/s` | `2748958.667 ops/s` | `-40.40%` | `worse` |
| `segment-index-get-live:getMissSync` | `4054246.793 ops/s` | `2454312.865 ops/s` | `-39.46%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `290257.091 ops/s` | `306732.132 ops/s` | `+5.68%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4479127.484 ops/s` | `2628143.990 ops/s` | `-41.32%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3652190.985 ops/s` | `2043184.123 ops/s` | `-44.06%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4683766.082 ops/s` | `2817056.245 ops/s` | `-39.85%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3120462.143 ops/s` | `2036844.334 ops/s` | `-34.73%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4940835.136 ops/s` | `2517757.636 ops/s` | `-49.04%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4371477.717 ops/s` | `2348001.880 ops/s` | `-46.29%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2143143.816 ops/s` | `1372498.965 ops/s` | `-35.96%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `277.943 ms/op` | `136.043 ms/op` | `-51.05%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `299.553 ms/op` | `155.771 ms/op` | `-48.00%` | `worse` |
| `segment-index-lifecycle:openExisting` | `272.378 ms/op` | `132.777 ms/op` | `-51.25%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `561082.312 ops/s` | `489995.931 ops/s` | `-12.67%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `282728.900 ops/s` | `218814.417 ops/s` | `-22.61%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `278353.412 ops/s` | `271181.514 ops/s` | `-2.58%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1234286.583 ops/s` | `1132759.013 ops/s` | `-8.23%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1214336.309 ops/s` | `1109134.664 ops/s` | `-8.66%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `19950.273 ops/s` | `23624.348 ops/s` | `+18.42%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8394.100 ops/s` | `4276.964 ops/s` | `-49.05%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8443.532 ops/s` | `4670.998 ops/s` | `-44.68%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3375.342 ops/s` | `2091.858 ops/s` | `-38.03%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3187.939 ops/s` | `2133.981 ops/s` | `-33.06%` | `worse` |
| `segment-index-range-scan:boundedScan` | `32.177 us/op` | `21.566 us/op` | `-32.98%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2107.331 us/op` | `1795.523 us/op` | `-14.80%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4053.673 us/op` | `3454.562 us/op` | `-14.78%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `358.814 us/op` | `304.480 us/op` | `-15.14%` | `worse` |
