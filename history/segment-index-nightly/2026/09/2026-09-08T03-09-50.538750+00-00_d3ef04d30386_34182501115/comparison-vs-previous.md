# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5036323.727 ops/s` | `4753356.398 ops/s` | `-5.62%` | `warning` |
| `segment-index-get-live:getMissSync` | `4829862.423 ops/s` | `4305794.361 ops/s` | `-10.85%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `269733.144 ops/s` | `284495.513 ops/s` | `+5.47%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4555348.571 ops/s` | `4646915.666 ops/s` | `+2.01%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3626580.555 ops/s` | `3429242.490 ops/s` | `-5.44%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4655959.924 ops/s` | `4155748.031 ops/s` | `-10.74%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3660990.711 ops/s` | `3350385.118 ops/s` | `-8.48%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4840660.467 ops/s` | `4640489.311 ops/s` | `-4.14%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `4596305.357 ops/s` | `4396237.361 ops/s` | `-4.35%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2163894.106 ops/s` | `2335019.678 ops/s` | `+7.91%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.100 ms/op` | `240.888 ms/op` | `-0.91%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `266.211 ms/op` | `263.491 ms/op` | `-1.02%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `239.933 ms/op` | `236.525 ms/op` | `-1.42%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `547792.832 ops/s` | `543419.270 ops/s` | `-0.80%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `286310.529 ops/s` | `265799.249 ops/s` | `-7.16%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `261482.303 ops/s` | `277620.021 ops/s` | `+6.17%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1259258.384 ops/s` | `1321174.376 ops/s` | `+4.92%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1238554.366 ops/s` | `1299519.878 ops/s` | `+4.92%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `20704.018 ops/s` | `21654.498 ops/s` | `+4.59%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7122.232 ops/s` | `7319.663 ops/s` | `+2.77%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7063.643 ops/s` | `7322.819 ops/s` | `+3.67%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2455.662 ops/s` | `2468.301 ops/s` | `+0.51%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2391.827 ops/s` | `2251.797 ops/s` | `-5.85%` | `warning` |
| `segment-index-range-scan:boundedScan` | `29.220 us/op` | `34.034 us/op` | `+16.47%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2006.538 us/op` | `1988.382 us/op` | `-0.90%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3934.287 us/op` | `4041.499 us/op` | `+2.73%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `331.984 us/op` | `331.764 us/op` | `-0.07%` | `neutral` |
