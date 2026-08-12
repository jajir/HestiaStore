# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `c45c9404bcc3cf298d00bcc44645697964949152`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4986455.979 ops/s` | `5127149.139 ops/s` | `+2.82%` | `neutral` |
| `segment-index-get-live:getMissSync` | `5062323.792 ops/s` | `4771921.255 ops/s` | `-5.74%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3330456.231 ops/s` | `3713602.432 ops/s` | `+11.50%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4566449.391 ops/s` | `4577553.423 ops/s` | `+0.24%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3268506.900 ops/s` | `3409636.305 ops/s` | `+4.32%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4624669.578 ops/s` | `4848166.931 ops/s` | `+4.83%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4217588.886 ops/s` | `4158349.694 ops/s` | `-1.40%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2310585.121 ops/s` | `2337168.812 ops/s` | `+1.15%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `609239.697 ops/s` | `589707.605 ops/s` | `-3.21%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `455755.478 ops/s` | `436430.743 ops/s` | `-4.24%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `153484.219 ops/s` | `153276.862 ops/s` | `-0.14%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `832500.544 ops/s` | `688549.179 ops/s` | `-17.29%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `819583.733 ops/s` | `677200.021 ops/s` | `-17.37%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12916.811 ops/s` | `11349.158 ops/s` | `-12.14%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6560.780 ops/s` | `6496.965 ops/s` | `-0.97%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6944.245 ops/s` | `6545.114 ops/s` | `-5.75%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2665.907 ops/s` | `2564.209 ops/s` | `-3.81%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2503.853 ops/s` | `2554.757 ops/s` | `+2.03%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `30.806 us/op` | `30.685 us/op` | `-0.39%` | `neutral` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2096.956 us/op` | `2163.749 us/op` | `+3.19%` | `better` |
| `segment-index-range-scan:sequentialRead` | `4065.472 us/op` | `4022.945 us/op` | `-1.05%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `323.958 us/op` | `324.470 us/op` | `+0.16%` | `neutral` |
