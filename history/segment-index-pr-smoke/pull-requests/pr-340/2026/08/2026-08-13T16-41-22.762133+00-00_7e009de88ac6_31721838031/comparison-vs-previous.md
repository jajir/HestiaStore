# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `7e009de88ac63762ae2d6889b74e2079a5639b47`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4986455.979 ops/s` | `5067914.817 ops/s` | `+1.63%` | `neutral` |
| `segment-index-get-live:getMissSync` | `5062323.792 ops/s` | `4857047.657 ops/s` | `-4.05%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3330456.231 ops/s` | `3652295.870 ops/s` | `+9.66%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4566449.391 ops/s` | `4529552.894 ops/s` | `-0.81%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3268506.900 ops/s` | `3464167.178 ops/s` | `+5.99%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4624669.578 ops/s` | `4864895.849 ops/s` | `+5.19%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4217588.886 ops/s` | `4322854.195 ops/s` | `+2.50%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2310585.121 ops/s` | `2294287.215 ops/s` | `-0.71%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `609239.697 ops/s` | `573797.098 ops/s` | `-5.82%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `455755.478 ops/s` | `396971.457 ops/s` | `-12.90%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `153484.219 ops/s` | `176825.641 ops/s` | `+15.21%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `832500.544 ops/s` | `675518.794 ops/s` | `-18.86%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `819583.733 ops/s` | `662289.322 ops/s` | `-19.19%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12916.811 ops/s` | `13229.472 ops/s` | `+2.42%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6560.780 ops/s` | `6029.403 ops/s` | `-8.10%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6944.245 ops/s` | `6542.741 ops/s` | `-5.78%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2665.907 ops/s` | `2565.154 ops/s` | `-3.78%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2503.853 ops/s` | `2453.889 ops/s` | `-2.00%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `30.806 us/op` | `33.595 us/op` | `+9.05%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2096.956 us/op` | `2239.438 us/op` | `+6.79%` | `better` |
| `segment-index-range-scan:sequentialRead` | `4065.472 us/op` | `4137.774 us/op` | `+1.78%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `323.958 us/op` | `323.402 us/op` | `-0.17%` | `neutral` |
