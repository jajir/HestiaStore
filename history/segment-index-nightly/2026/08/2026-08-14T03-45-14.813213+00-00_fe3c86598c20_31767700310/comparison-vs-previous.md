# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2511949.297 ops/s` | `2987874.916 ops/s` | `+18.95%` | `better` |
| `segment-index-get-live:getMissSync` | `2665756.889 ops/s` | `2417427.065 ops/s` | `-9.32%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `331087.830 ops/s` | `302680.119 ops/s` | `-8.58%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `2667790.232 ops/s` | `2682725.966 ops/s` | `+0.56%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `2289869.073 ops/s` | `1987969.037 ops/s` | `-13.18%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2779268.314 ops/s` | `2732249.075 ops/s` | `-1.69%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1958839.289 ops/s` | `1789822.114 ops/s` | `-8.63%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2521108.963 ops/s` | `2729407.885 ops/s` | `+8.26%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3064849.442 ops/s` | `2524611.653 ops/s` | `-17.63%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1514339.004 ops/s` | `1576181.545 ops/s` | `+4.08%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `117.894 ms/op` | `135.793 ms/op` | `+15.18%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `134.198 ms/op` | `157.562 ms/op` | `+17.41%` | `better` |
| `segment-index-lifecycle:openExisting` | `113.423 ms/op` | `132.629 ms/op` | `+16.93%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `515494.525 ops/s` | `468082.498 ops/s` | `-9.20%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `244502.478 ops/s` | `214728.595 ops/s` | `-12.18%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `270992.048 ops/s` | `253353.903 ops/s` | `-6.51%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1165509.983 ops/s` | `1131388.355 ops/s` | `-2.93%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1148859.059 ops/s` | `1113984.939 ops/s` | `-3.04%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16650.924 ops/s` | `17403.416 ops/s` | `+4.52%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `1927.689 ops/s` | `4948.739 ops/s` | `+156.72%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `1600.849 ops/s` | `4779.397 ops/s` | `+198.55%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1546.967 ops/s` | `2123.185 ops/s` | `+37.25%` | `better` |
| `segment-index-persisted-mutation:putSync` | `819.618 ops/s` | `2073.446 ops/s` | `+152.98%` | `better` |
| `segment-index-range-scan:boundedScan` | `22.083 us/op` | `24.508 us/op` | `+10.98%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1627.146 us/op` | `1806.837 us/op` | `+11.04%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3130.370 us/op` | `3554.100 us/op` | `+13.54%` | `better` |
| `segment-merge-sequential:mergeSequential` | `264.326 us/op` | `304.015 us/op` | `+15.02%` | `better` |
