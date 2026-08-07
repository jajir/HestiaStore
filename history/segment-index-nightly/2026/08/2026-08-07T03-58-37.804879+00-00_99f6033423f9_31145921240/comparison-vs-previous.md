# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4806440.449 ops/s` | `3022163.810 ops/s` | `-37.12%` | `worse` |
| `segment-index-get-live:getMissSync` | `4910644.966 ops/s` | `3364191.324 ops/s` | `-31.49%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `290734.876 ops/s` | `266507.948 ops/s` | `-8.33%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `4870068.333 ops/s` | `2842845.506 ops/s` | `-41.63%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3700299.349 ops/s` | `2347575.805 ops/s` | `-36.56%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4719058.401 ops/s` | `2967618.630 ops/s` | `-37.11%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3526907.895 ops/s` | `2192855.093 ops/s` | `-37.82%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4677489.162 ops/s` | `3121625.549 ops/s` | `-33.26%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4377921.711 ops/s` | `2884072.674 ops/s` | `-34.12%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2281667.178 ops/s` | `1448224.699 ops/s` | `-36.53%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `238.569 ms/op` | `130.352 ms/op` | `-45.36%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `259.898 ms/op` | `153.545 ms/op` | `-40.92%` | `worse` |
| `segment-index-lifecycle:openExisting` | `236.215 ms/op` | `126.684 ms/op` | `-46.37%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `546735.015 ops/s` | `452966.215 ops/s` | `-17.15%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `296328.295 ops/s` | `220569.700 ops/s` | `-25.57%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `250406.720 ops/s` | `232396.515 ops/s` | `-7.19%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1173482.059 ops/s` | `997715.275 ops/s` | `-14.98%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1159173.123 ops/s` | `982736.238 ops/s` | `-15.22%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14308.936 ops/s` | `14979.037 ops/s` | `+4.68%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7505.288 ops/s` | `8382.185 ops/s` | `+11.68%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7717.680 ops/s` | `8354.639 ops/s` | `+8.25%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2583.589 ops/s` | `3205.494 ops/s` | `+24.07%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2478.597 ops/s` | `3053.159 ops/s` | `+23.18%` | `better` |
| `segment-index-range-scan:boundedScan` | `30.156 us/op` | `24.236 us/op` | `-19.63%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1986.823 us/op` | `2061.483 us/op` | `+3.76%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3898.834 us/op` | `3890.477 us/op` | `-0.21%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `327.961 us/op` | `332.115 us/op` | `+1.27%` | `neutral` |
