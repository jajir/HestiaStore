# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6167010.924 ops/s` | `6247444.027 ops/s` | `+1.30%` | `neutral` |
| `segment-index-get-live:getMissSync` | `6713107.739 ops/s` | `5133148.508 ops/s` | `-23.54%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `474162.075 ops/s` | `365330.004 ops/s` | `-22.95%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `6589640.233 ops/s` | `5136755.469 ops/s` | `-22.05%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `5144668.972 ops/s` | `4562817.505 ops/s` | `-11.31%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `6192154.187 ops/s` | `5788678.569 ops/s` | `-6.52%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `5321859.883 ops/s` | `4257830.088 ops/s` | `-19.99%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `6616327.033 ops/s` | `5549375.542 ops/s` | `-16.13%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `6070464.367 ops/s` | `5150241.065 ops/s` | `-15.16%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `3057247.958 ops/s` | `2756765.253 ops/s` | `-9.83%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `201.698 ms/op` | `212.093 ms/op` | `+5.15%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `221.743 ms/op` | `225.353 ms/op` | `+1.63%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `203.795 ms/op` | `209.403 ms/op` | `+2.75%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `678769.801 ops/s` | `699352.395 ops/s` | `+3.03%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `394317.990 ops/s` | `375451.417 ops/s` | `-4.78%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `284451.811 ops/s` | `323900.978 ops/s` | `+13.87%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1993251.996 ops/s` | `1487514.493 ops/s` | `-25.37%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1945027.260 ops/s` | `1445647.014 ops/s` | `-25.67%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `48224.736 ops/s` | `41867.479 ops/s` | `-13.18%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6175.005 ops/s` | `9877.852 ops/s` | `+59.97%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7051.986 ops/s` | `9668.632 ops/s` | `+37.11%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2789.507 ops/s` | `4571.645 ops/s` | `+63.89%` | `better` |
| `segment-index-persisted-mutation:putSync` | `3263.992 ops/s` | `4371.799 ops/s` | `+33.94%` | `better` |
| `segment-index-range-scan:boundedScan` | `19.268 us/op` | `20.721 us/op` | `+7.54%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1281.915 us/op` | `1526.828 us/op` | `+19.11%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2374.523 us/op` | `2957.239 us/op` | `+24.54%` | `better` |
| `segment-merge-sequential:mergeSequential` | `182.877 us/op` | `280.586 us/op` | `+53.43%` | `better` |
