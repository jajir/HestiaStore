# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Candidate SHA: `0273b72f625d972562943f50e90d2e06f57a34e4`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4969144.026 ops/s` | `5082625.331 ops/s` | `+2.28%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4386179.172 ops/s` | `4687113.821 ops/s` | `+6.86%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3362674.691 ops/s` | `3374306.162 ops/s` | `+0.35%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4620219.782 ops/s` | `4657294.034 ops/s` | `+0.80%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3327735.634 ops/s` | `3585360.090 ops/s` | `+7.74%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4543708.340 ops/s` | `4737169.284 ops/s` | `+4.26%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3918903.550 ops/s` | `4103892.849 ops/s` | `+4.72%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2076680.733 ops/s` | `2245504.088 ops/s` | `+8.13%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `581134.535 ops/s` | `604269.708 ops/s` | `+3.98%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `399639.738 ops/s` | `433328.573 ops/s` | `+8.43%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181494.797 ops/s` | `170941.134 ops/s` | `-5.81%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `952536.649 ops/s` | `767983.124 ops/s` | `-19.37%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `931688.334 ops/s` | `749841.634 ops/s` | `-19.52%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `20848.315 ops/s` | `18141.490 ops/s` | `-12.98%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7805.742 ops/s` | `6941.353 ops/s` | `-11.07%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7779.312 ops/s` | `6919.072 ops/s` | `-11.06%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3353.603 ops/s` | `2828.140 ops/s` | `-15.67%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3564.405 ops/s` | `2854.494 ops/s` | `-19.92%` | `worse` |
| `segment-index-range-scan:boundedScan` | `30.065 us/op` | `27.726 us/op` | `-7.78%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2179.389 us/op` | `2081.602 us/op` | `-4.49%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `4198.916 us/op` | `4231.729 us/op` | `+0.78%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `349.508 us/op` | `324.567 us/op` | `-7.14%` | `worse` |
