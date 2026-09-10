# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3178355.996 ops/s` | `4366872.202 ops/s` | `+37.39%` | `better` |
| `segment-index-get-live:getMissSync` | `2881607.466 ops/s` | `4202731.085 ops/s` | `+45.85%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `312083.650 ops/s` | `285136.899 ops/s` | `-8.63%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `2748571.489 ops/s` | `4447669.352 ops/s` | `+61.82%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `1645537.476 ops/s` | `3424294.128 ops/s` | `+108.10%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2809745.482 ops/s` | `4723682.215 ops/s` | `+68.12%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1980777.626 ops/s` | `3415291.966 ops/s` | `+72.42%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3065119.431 ops/s` | `4464686.982 ops/s` | `+45.66%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2521374.372 ops/s` | `4015341.391 ops/s` | `+59.25%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1396785.023 ops/s` | `2149260.887 ops/s` | `+53.87%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `127.330 ms/op` | `274.939 ms/op` | `+115.93%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `148.389 ms/op` | `299.114 ms/op` | `+101.57%` | `better` |
| `segment-index-lifecycle:openExisting` | `124.321 ms/op` | `271.393 ms/op` | `+118.30%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `507554.173 ops/s` | `566580.783 ops/s` | `+11.63%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `231567.610 ops/s` | `280750.246 ops/s` | `+21.24%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `275986.563 ops/s` | `285830.537 ops/s` | `+3.57%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1163994.544 ops/s` | `1312383.582 ops/s` | `+12.75%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1140411.230 ops/s` | `1291473.739 ops/s` | `+13.25%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `23583.314 ops/s` | `20909.843 ops/s` | `-11.34%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `4393.983 ops/s` | `7888.205 ops/s` | `+79.52%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `4710.138 ops/s` | `7791.823 ops/s` | `+65.43%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1882.227 ops/s` | `3150.101 ops/s` | `+67.36%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1788.232 ops/s` | `3077.620 ops/s` | `+72.10%` | `better` |
| `segment-index-range-scan:boundedScan` | `21.940 us/op` | `29.817 us/op` | `+35.90%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1784.314 us/op` | `2108.303 us/op` | `+18.16%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3456.839 us/op` | `4050.324 us/op` | `+17.17%` | `better` |
| `segment-merge-sequential:mergeSequential` | `291.159 us/op` | `359.489 us/op` | `+23.47%` | `better` |
