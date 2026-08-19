# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `b20bc5b3bbd7c2ca573d925820e56e7a9db8b1ac`
- Candidate SHA: `fd363b3decebe0bbb7dbad78f3283046a8572cf6`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4918392.041 ops/s` | `5055323.338 ops/s` | `+2.78%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4347487.557 ops/s` | `4883252.115 ops/s` | `+12.32%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3364619.807 ops/s` | `3538369.262 ops/s` | `+5.16%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4530456.056 ops/s` | `4786760.821 ops/s` | `+5.66%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3556335.868 ops/s` | `3216526.192 ops/s` | `-9.56%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4498146.909 ops/s` | `4657659.698 ops/s` | `+3.55%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3930878.020 ops/s` | `4178451.401 ops/s` | `+6.30%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2056526.903 ops/s` | `2202231.759 ops/s` | `+7.08%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `547544.185 ops/s` | `515633.695 ops/s` | `-5.83%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `345528.155 ops/s` | `324505.980 ops/s` | `-6.08%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `202016.030 ops/s` | `191127.715 ops/s` | `-5.39%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `893678.659 ops/s` | `915586.547 ops/s` | `+2.45%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `874195.972 ops/s` | `895116.387 ops/s` | `+2.39%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `19482.687 ops/s` | `20470.161 ops/s` | `+5.07%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7168.223 ops/s` | `5943.698 ops/s` | `-17.08%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7389.579 ops/s` | `6206.954 ops/s` | `-16.00%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3230.971 ops/s` | `2475.900 ops/s` | `-23.37%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3061.779 ops/s` | `2465.116 ops/s` | `-19.49%` | `worse` |
| `segment-index-range-scan:boundedScan` | `30.293 us/op` | `30.584 us/op` | `+0.96%` | `neutral` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2179.621 us/op` | `2201.235 us/op` | `+0.99%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4244.928 us/op` | `4034.414 us/op` | `-4.96%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `352.704 us/op` | `324.072 us/op` | `-8.12%` | `worse` |
