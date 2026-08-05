# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `2a6f331b98ad6c43ed2b0d06b0107d002643ff17`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5161799.173 ops/s` | `4757246.093 ops/s` | `-7.84%` | `worse` |
| `segment-index-get-live:getMissSync` | `4724919.726 ops/s` | `4425814.718 ops/s` | `-6.33%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3546583.504 ops/s` | `3422995.585 ops/s` | `-3.48%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4626594.096 ops/s` | `4553420.102 ops/s` | `-1.58%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3647597.060 ops/s` | `3356735.618 ops/s` | `-7.97%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4662914.482 ops/s` | `4597344.303 ops/s` | `-1.41%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4224176.217 ops/s` | `3982055.740 ops/s` | `-5.73%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2191504.089 ops/s` | `2036075.875 ops/s` | `-7.09%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `549239.192 ops/s` | `555585.815 ops/s` | `+1.16%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `371670.329 ops/s` | `396898.562 ops/s` | `+6.79%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `177568.863 ops/s` | `158687.253 ops/s` | `-10.63%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `813740.977 ops/s` | `866358.023 ops/s` | `+6.47%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `799487.666 ops/s` | `852591.636 ops/s` | `+6.64%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14253.311 ops/s` | `13766.387 ops/s` | `-3.42%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6755.614 ops/s` | `7226.297 ops/s` | `+6.97%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6574.037 ops/s` | `7619.501 ops/s` | `+15.90%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2691.104 ops/s` | `3396.094 ops/s` | `+26.20%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2717.828 ops/s` | `3173.929 ops/s` | `+16.78%` | `better` |
| `segment-index-range-scan:boundedScan` | `29.672 us/op` | `30.829 us/op` | `+3.90%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2105.571 us/op` | `2241.162 us/op` | `+6.44%` | `better` |
| `segment-index-range-scan:sequentialRead` | `4149.767 us/op` | `4534.669 us/op` | `+9.28%` | `better` |
| `segment-merge-sequential:mergeSequential` | `325.386 us/op` | `349.648 us/op` | `+7.46%` | `better` |
