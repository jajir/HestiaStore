# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5161799.173 ops/s` | `4986455.979 ops/s` | `-3.40%` | `warning` |
| `segment-index-get-live:getMissSync` | `4724919.726 ops/s` | `5062323.792 ops/s` | `+7.14%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3546583.504 ops/s` | `3330456.231 ops/s` | `-6.09%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4626594.096 ops/s` | `4566449.391 ops/s` | `-1.30%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3647597.060 ops/s` | `3268506.900 ops/s` | `-10.39%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4662914.482 ops/s` | `4624669.578 ops/s` | `-0.82%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4224176.217 ops/s` | `4217588.886 ops/s` | `-0.16%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2191504.089 ops/s` | `2310585.121 ops/s` | `+5.43%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `549239.192 ops/s` | `609239.697 ops/s` | `+10.92%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `371670.329 ops/s` | `455755.478 ops/s` | `+22.62%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `177568.863 ops/s` | `153484.219 ops/s` | `-13.56%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `813740.977 ops/s` | `832500.544 ops/s` | `+2.31%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `799487.666 ops/s` | `819583.733 ops/s` | `+2.51%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14253.311 ops/s` | `12916.811 ops/s` | `-9.38%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6755.614 ops/s` | `6560.780 ops/s` | `-2.88%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6574.037 ops/s` | `6944.245 ops/s` | `+5.63%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2691.104 ops/s` | `2665.907 ops/s` | `-0.94%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2717.828 ops/s` | `2503.853 ops/s` | `-7.87%` | `worse` |
| `segment-index-range-scan:boundedScan` | `29.672 us/op` | `30.806 us/op` | `+3.82%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2105.571 us/op` | `2096.956 us/op` | `-0.41%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4149.767 us/op` | `4065.472 us/op` | `-2.03%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `325.386 us/op` | `323.958 us/op` | `-0.44%` | `neutral` |
