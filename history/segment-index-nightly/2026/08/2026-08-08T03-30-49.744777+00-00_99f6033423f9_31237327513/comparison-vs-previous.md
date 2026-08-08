# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3022163.810 ops/s` | `3112669.116 ops/s` | `+2.99%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3364191.324 ops/s` | `3001687.960 ops/s` | `-10.78%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `266507.948 ops/s` | `352021.192 ops/s` | `+32.09%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `2842845.506 ops/s` | `3183234.252 ops/s` | `+11.97%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `2347575.805 ops/s` | `1966855.517 ops/s` | `-16.22%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2967618.630 ops/s` | `3202366.829 ops/s` | `+7.91%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2192855.093 ops/s` | `1829760.414 ops/s` | `-16.56%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3121625.549 ops/s` | `2574652.003 ops/s` | `-17.52%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2884072.674 ops/s` | `2547560.298 ops/s` | `-11.67%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1448224.699 ops/s` | `1492076.264 ops/s` | `+3.03%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `130.352 ms/op` | `112.736 ms/op` | `-13.51%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `153.545 ms/op` | `130.411 ms/op` | `-15.07%` | `worse` |
| `segment-index-lifecycle:openExisting` | `126.684 ms/op` | `112.100 ms/op` | `-11.51%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `452966.215 ops/s` | `533181.715 ops/s` | `+17.71%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `220569.700 ops/s` | `264908.213 ops/s` | `+20.10%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `232396.515 ops/s` | `268273.502 ops/s` | `+15.44%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `997715.275 ops/s` | `1209188.224 ops/s` | `+21.20%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `982736.238 ops/s` | `1191016.198 ops/s` | `+21.19%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14979.037 ops/s` | `18172.026 ops/s` | `+21.32%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8382.185 ops/s` | `3881.978 ops/s` | `-53.69%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8354.639 ops/s` | `4019.710 ops/s` | `-51.89%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3205.494 ops/s` | `1741.091 ops/s` | `-45.68%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3053.159 ops/s` | `1874.444 ops/s` | `-38.61%` | `worse` |
| `segment-index-range-scan:boundedScan` | `24.236 us/op` | `20.345 us/op` | `-16.05%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2061.483 us/op` | `1606.715 us/op` | `-22.06%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3890.477 us/op` | `2991.268 us/op` | `-23.11%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `332.115 us/op` | `262.976 us/op` | `-20.82%` | `worse` |
