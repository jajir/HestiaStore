# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4753356.398 ops/s` | `3178355.996 ops/s` | `-33.13%` | `worse` |
| `segment-index-get-live:getMissSync` | `4305794.361 ops/s` | `2881607.466 ops/s` | `-33.08%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `284495.513 ops/s` | `312083.650 ops/s` | `+9.70%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4646915.666 ops/s` | `2748571.489 ops/s` | `-40.85%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3429242.490 ops/s` | `1645537.476 ops/s` | `-52.01%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4155748.031 ops/s` | `2809745.482 ops/s` | `-32.39%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3350385.118 ops/s` | `1980777.626 ops/s` | `-40.88%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4640489.311 ops/s` | `3065119.431 ops/s` | `-33.95%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4396237.361 ops/s` | `2521374.372 ops/s` | `-42.65%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2335019.678 ops/s` | `1396785.023 ops/s` | `-40.18%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `240.888 ms/op` | `127.330 ms/op` | `-47.14%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `263.491 ms/op` | `148.389 ms/op` | `-43.68%` | `worse` |
| `segment-index-lifecycle:openExisting` | `236.525 ms/op` | `124.321 ms/op` | `-47.44%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `543419.270 ops/s` | `507554.173 ops/s` | `-6.60%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `265799.249 ops/s` | `231567.610 ops/s` | `-12.88%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `277620.021 ops/s` | `275986.563 ops/s` | `-0.59%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1321174.376 ops/s` | `1163994.544 ops/s` | `-11.90%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1299519.878 ops/s` | `1140411.230 ops/s` | `-12.24%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `21654.498 ops/s` | `23583.314 ops/s` | `+8.91%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7319.663 ops/s` | `4393.983 ops/s` | `-39.97%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7322.819 ops/s` | `4710.138 ops/s` | `-35.68%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2468.301 ops/s` | `1882.227 ops/s` | `-23.74%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2251.797 ops/s` | `1788.232 ops/s` | `-20.59%` | `worse` |
| `segment-index-range-scan:boundedScan` | `34.034 us/op` | `21.940 us/op` | `-35.54%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1988.382 us/op` | `1784.314 us/op` | `-10.26%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4041.499 us/op` | `3456.839 us/op` | `-14.47%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `331.764 us/op` | `291.159 us/op` | `-12.24%` | `worse` |
