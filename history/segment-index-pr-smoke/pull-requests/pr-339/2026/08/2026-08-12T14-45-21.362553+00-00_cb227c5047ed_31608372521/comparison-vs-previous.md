# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `cb227c5047edfcb05f014ddf42c727cd3c899778`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4986455.979 ops/s` | `4903667.859 ops/s` | `-1.66%` | `neutral` |
| `segment-index-get-live:getMissSync` | `5062323.792 ops/s` | `4937963.429 ops/s` | `-2.46%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3330456.231 ops/s` | `3288626.866 ops/s` | `-1.26%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4566449.391 ops/s` | `4579109.491 ops/s` | `+0.28%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3268506.900 ops/s` | `3460400.326 ops/s` | `+5.87%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4624669.578 ops/s` | `4627914.729 ops/s` | `+0.07%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4217588.886 ops/s` | `4260737.368 ops/s` | `+1.02%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2310585.121 ops/s` | `2267965.890 ops/s` | `-1.84%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `609239.697 ops/s` | `618465.868 ops/s` | `+1.51%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `455755.478 ops/s` | `449079.659 ops/s` | `-1.46%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `153484.219 ops/s` | `169386.209 ops/s` | `+10.36%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `832500.544 ops/s` | `853939.590 ops/s` | `+2.58%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `819583.733 ops/s` | `838571.185 ops/s` | `+2.32%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12916.811 ops/s` | `15368.405 ops/s` | `+18.98%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6560.780 ops/s` | `5967.011 ops/s` | `-9.05%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6944.245 ops/s` | `6395.994 ops/s` | `-7.90%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2665.907 ops/s` | `2519.533 ops/s` | `-5.49%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2503.853 ops/s` | `2393.377 ops/s` | `-4.41%` | `warning` |
| `segment-index-range-scan:boundedScan` | `30.806 us/op` | `28.694 us/op` | `-6.85%` | `warning` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2096.956 us/op` | `2088.742 us/op` | `-0.39%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4065.472 us/op` | `4162.970 us/op` | `+2.40%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `323.958 us/op` | `325.112 us/op` | `+0.36%` | `neutral` |
