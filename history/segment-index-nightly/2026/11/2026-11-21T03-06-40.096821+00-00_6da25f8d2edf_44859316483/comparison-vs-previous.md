# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6247444.027 ops/s` | `4581738.007 ops/s` | `-26.66%` | `worse` |
| `segment-index-get-live:getMissSync` | `5133148.508 ops/s` | `4302259.672 ops/s` | `-16.19%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `365330.004 ops/s` | `279648.980 ops/s` | `-23.45%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `5136755.469 ops/s` | `4807860.971 ops/s` | `-6.40%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `4562817.505 ops/s` | `3377848.379 ops/s` | `-25.97%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `5788678.569 ops/s` | `4332331.463 ops/s` | `-25.16%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `4257830.088 ops/s` | `3212520.252 ops/s` | `-24.55%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `5549375.542 ops/s` | `4505444.064 ops/s` | `-18.81%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `5150241.065 ops/s` | `3860201.473 ops/s` | `-25.05%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2756765.253 ops/s` | `2126613.869 ops/s` | `-22.86%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `212.093 ms/op` | `234.275 ms/op` | `+10.46%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `225.353 ms/op` | `254.264 ms/op` | `+12.83%` | `better` |
| `segment-index-lifecycle:openExisting` | `209.403 ms/op` | `231.264 ms/op` | `+10.44%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `699352.395 ops/s` | `568876.967 ops/s` | `-18.66%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `375451.417 ops/s` | `276139.141 ops/s` | `-26.45%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `323900.978 ops/s` | `292737.825 ops/s` | `-9.62%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1487514.493 ops/s` | `1232094.016 ops/s` | `-17.17%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1445647.014 ops/s` | `1198436.293 ops/s` | `-17.10%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `41867.479 ops/s` | `33657.724 ops/s` | `-19.61%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `9877.852 ops/s` | `7373.181 ops/s` | `-25.36%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `9668.632 ops/s` | `7444.552 ops/s` | `-23.00%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `4571.645 ops/s` | `3245.555 ops/s` | `-29.01%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `4371.799 ops/s` | `3109.952 ops/s` | `-28.86%` | `worse` |
| `segment-index-range-scan:boundedScan` | `20.721 us/op` | `30.801 us/op` | `+48.65%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1526.828 us/op` | `2052.559 us/op` | `+34.43%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2957.239 us/op` | `3853.580 us/op` | `+30.31%` | `better` |
| `segment-merge-sequential:mergeSequential` | `280.586 us/op` | `332.872 us/op` | `+18.63%` | `better` |
