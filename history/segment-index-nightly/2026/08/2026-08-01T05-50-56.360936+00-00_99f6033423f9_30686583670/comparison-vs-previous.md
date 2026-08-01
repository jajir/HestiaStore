# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5283369.640 ops/s` | `6281627.959 ops/s` | `+18.89%` | `better` |
| `segment-index-get-live:getMissSync` | `4342179.377 ops/s` | `6306770.152 ops/s` | `+45.24%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `277972.193 ops/s` | `373610.592 ops/s` | `+34.41%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4749656.009 ops/s` | `5695634.390 ops/s` | `+19.92%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3584209.518 ops/s` | `4323237.072 ops/s` | `+20.62%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4635316.033 ops/s` | `5949154.641 ops/s` | `+28.34%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3813667.923 ops/s` | `4251874.605 ops/s` | `+11.49%` | `better` |
| `segment-index-get-persisted:getMissSync` | `5153795.832 ops/s` | `5716299.038 ops/s` | `+10.91%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4274847.289 ops/s` | `5127229.782 ops/s` | `+19.94%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2348899.400 ops/s` | `2588903.598 ops/s` | `+10.22%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `256.158 ms/op` | `239.447 ms/op` | `-6.52%` | `warning` |
| `segment-index-lifecycle:openAndCompact` | `279.320 ms/op` | `257.065 ms/op` | `-7.97%` | `worse` |
| `segment-index-lifecycle:openExisting` | `252.226 ms/op` | `236.281 ms/op` | `-6.32%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `519361.234 ops/s` | `628212.219 ops/s` | `+20.96%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `271956.777 ops/s` | `338700.659 ops/s` | `+24.54%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `247404.457 ops/s` | `289511.560 ops/s` | `+17.02%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1196571.576 ops/s` | `1705311.055 ops/s` | `+42.52%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1181289.901 ops/s` | `1688513.655 ops/s` | `+42.94%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15281.675 ops/s` | `16797.399 ops/s` | `+9.92%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6723.070 ops/s` | `5871.922 ops/s` | `-12.66%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6824.380 ops/s` | `5898.426 ops/s` | `-13.57%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2387.474 ops/s` | `2445.103 ops/s` | `+2.41%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2238.750 ops/s` | `2277.100 ops/s` | `+1.71%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `31.401 us/op` | `23.046 us/op` | `-26.61%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2112.223 us/op` | `1600.335 us/op` | `-24.23%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4039.157 us/op` | `3102.165 us/op` | `-23.20%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `332.950 us/op` | `279.385 us/op` | `-16.09%` | `worse` |
