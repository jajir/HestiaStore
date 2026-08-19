# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `59ebbf0b8215d9998c6f914a8a8590d2a5c601a9`
- Candidate SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4923566.745 ops/s` | `4969144.026 ops/s` | `+0.93%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4536858.131 ops/s` | `4386179.172 ops/s` | `-3.32%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3617352.554 ops/s` | `3362674.691 ops/s` | `-7.04%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4385016.851 ops/s` | `4620219.782 ops/s` | `+5.36%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3396930.052 ops/s` | `3327735.634 ops/s` | `-2.04%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4678508.650 ops/s` | `4543708.340 ops/s` | `-2.88%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4026268.873 ops/s` | `3918903.550 ops/s` | `-2.67%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2045480.807 ops/s` | `2076680.733 ops/s` | `+1.53%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `600288.151 ops/s` | `581134.535 ops/s` | `-3.19%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `429239.614 ops/s` | `399639.738 ops/s` | `-6.90%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `171048.537 ops/s` | `181494.797 ops/s` | `+6.11%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `692253.430 ops/s` | `952536.649 ops/s` | `+37.60%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `674346.936 ops/s` | `931688.334 ops/s` | `+38.16%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17906.494 ops/s` | `20848.315 ops/s` | `+16.43%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7437.105 ops/s` | `7805.742 ops/s` | `+4.96%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7418.110 ops/s` | `7779.312 ops/s` | `+4.87%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3591.261 ops/s` | `3353.603 ops/s` | `-6.62%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `3651.881 ops/s` | `3564.405 ops/s` | `-2.40%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `39.110 us/op` | `30.065 us/op` | `-23.13%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2184.486 us/op` | `2179.389 us/op` | `-0.23%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4323.035 us/op` | `4198.916 us/op` | `-2.87%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `350.172 us/op` | `349.508 us/op` | `-0.19%` | `neutral` |
