# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `fe3c86598c2019fd5ff45fef2635519ddcc4ebb3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4986455.979 ops/s` | `3111428.501 ops/s` | `-37.60%` | `worse` |
| `segment-index-get-live:getMissSync` | `5062323.792 ops/s` | `2967309.942 ops/s` | `-41.38%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3330456.231 ops/s` | `2039836.099 ops/s` | `-38.75%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4566449.391 ops/s` | `2865984.950 ops/s` | `-37.24%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3268506.900 ops/s` | `2037517.579 ops/s` | `-37.66%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4624669.578 ops/s` | `2178613.353 ops/s` | `-52.89%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4217588.886 ops/s` | `2590461.880 ops/s` | `-38.58%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2310585.121 ops/s` | `1405255.168 ops/s` | `-39.18%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `609239.697 ops/s` | `533282.513 ops/s` | `-12.47%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `455755.478 ops/s` | `343848.570 ops/s` | `-24.55%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `153484.219 ops/s` | `189433.943 ops/s` | `+23.42%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `832500.544 ops/s` | `840436.735 ops/s` | `+0.95%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `819583.733 ops/s` | `824560.793 ops/s` | `+0.61%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12916.811 ops/s` | `15875.942 ops/s` | `+22.91%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6560.780 ops/s` | `681.872 ops/s` | `-89.61%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6944.245 ops/s` | `2259.467 ops/s` | `-67.46%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2665.907 ops/s` | `210.609 ops/s` | `-92.10%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2503.853 ops/s` | `456.764 ops/s` | `-81.76%` | `worse` |
| `segment-index-range-scan:boundedScan` | `30.806 us/op` | `22.058 us/op` | `-28.40%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2096.956 us/op` | `1668.636 us/op` | `-20.43%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4065.472 us/op` | `3259.736 us/op` | `-19.82%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `323.958 us/op` | `269.595 us/op` | `-16.78%` | `worse` |
