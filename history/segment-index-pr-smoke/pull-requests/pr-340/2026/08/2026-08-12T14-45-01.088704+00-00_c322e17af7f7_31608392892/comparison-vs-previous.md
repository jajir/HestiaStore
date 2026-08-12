# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `c322e17af7f719d468d1999108ea13ae506ee9e8`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4986455.979 ops/s` | `3026571.245 ops/s` | `-39.30%` | `worse` |
| `segment-index-get-live:getMissSync` | `5062323.792 ops/s` | `2733312.781 ops/s` | `-46.01%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3330456.231 ops/s` | `2322329.150 ops/s` | `-30.27%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4566449.391 ops/s` | `2818576.847 ops/s` | `-38.28%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3268506.900 ops/s` | `2305985.889 ops/s` | `-29.45%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4624669.578 ops/s` | `2438685.855 ops/s` | `-47.27%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4217588.886 ops/s` | `2586251.078 ops/s` | `-38.68%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2310585.121 ops/s` | `1376368.318 ops/s` | `-40.43%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `609239.697 ops/s` | `562517.116 ops/s` | `-7.67%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `455755.478 ops/s` | `328339.846 ops/s` | `-27.96%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `153484.219 ops/s` | `234177.269 ops/s` | `+52.57%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `832500.544 ops/s` | `1091386.927 ops/s` | `+31.10%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `819583.733 ops/s` | `1071805.437 ops/s` | `+30.77%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12916.811 ops/s` | `19581.490 ops/s` | `+51.60%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6560.780 ops/s` | `1032.369 ops/s` | `-84.26%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6944.245 ops/s` | `473.032 ops/s` | `-93.19%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2665.907 ops/s` | `223.449 ops/s` | `-91.62%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2503.853 ops/s` | `143.096 ops/s` | `-94.28%` | `worse` |
| `segment-index-range-scan:boundedScan` | `30.806 us/op` | `17.500 us/op` | `-43.19%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2096.956 us/op` | `1464.366 us/op` | `-30.17%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4065.472 us/op` | `2821.451 us/op` | `-30.60%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `323.958 us/op` | `219.091 us/op` | `-32.37%` | `worse` |
