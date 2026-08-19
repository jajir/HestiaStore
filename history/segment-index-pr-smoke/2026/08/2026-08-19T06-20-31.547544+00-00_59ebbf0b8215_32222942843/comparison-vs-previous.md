# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `fd363b3decebe0bbb7dbad78f3283046a8572cf6`
- Candidate SHA: `59ebbf0b8215d9998c6f914a8a8590d2a5c601a9`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5055323.338 ops/s` | `4923566.745 ops/s` | `-2.61%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4883252.115 ops/s` | `4536858.131 ops/s` | `-7.09%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3538369.262 ops/s` | `3617352.554 ops/s` | `+2.23%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4786760.821 ops/s` | `4385016.851 ops/s` | `-8.39%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3216526.192 ops/s` | `3396930.052 ops/s` | `+5.61%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4657659.698 ops/s` | `4678508.650 ops/s` | `+0.45%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4178451.401 ops/s` | `4026268.873 ops/s` | `-3.64%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2202231.759 ops/s` | `2045480.807 ops/s` | `-7.12%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `515633.695 ops/s` | `600288.151 ops/s` | `+16.42%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `324505.980 ops/s` | `429239.614 ops/s` | `+32.27%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `191127.715 ops/s` | `171048.537 ops/s` | `-10.51%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `915586.547 ops/s` | `692253.430 ops/s` | `-24.39%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `895116.387 ops/s` | `674346.936 ops/s` | `-24.66%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `20470.161 ops/s` | `17906.494 ops/s` | `-12.52%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `5943.698 ops/s` | `7437.105 ops/s` | `+25.13%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6206.954 ops/s` | `7418.110 ops/s` | `+19.51%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2475.900 ops/s` | `3591.261 ops/s` | `+45.05%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2465.116 ops/s` | `3651.881 ops/s` | `+48.14%` | `better` |
| `segment-index-range-scan:boundedScan` | `30.584 us/op` | `39.110 us/op` | `+27.88%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2201.235 us/op` | `2184.486 us/op` | `-0.76%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4034.414 us/op` | `4323.035 us/op` | `+7.15%` | `better` |
| `segment-merge-sequential:mergeSequential` | `324.072 us/op` | `350.172 us/op` | `+8.05%` | `better` |
