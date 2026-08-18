# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `bf830eb6be92f45201d79a8df2c45b2570dec148`
- Candidate SHA: `b20bc5b3bbd7c2ca573d925820e56e7a9db8b1ac`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4930000.319 ops/s` | `4918392.041 ops/s` | `-0.24%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4863914.805 ops/s` | `4347487.557 ops/s` | `-10.62%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3338864.105 ops/s` | `3364619.807 ops/s` | `+0.77%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4553286.052 ops/s` | `4530456.056 ops/s` | `-0.50%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3300569.917 ops/s` | `3556335.868 ops/s` | `+7.75%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4687755.521 ops/s` | `4498146.909 ops/s` | `-4.04%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `4275082.908 ops/s` | `3930878.020 ops/s` | `-8.05%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2179288.776 ops/s` | `2056526.903 ops/s` | `-5.63%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `600171.571 ops/s` | `547544.185 ops/s` | `-8.77%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `426096.053 ops/s` | `345528.155 ops/s` | `-18.91%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `174075.517 ops/s` | `202016.030 ops/s` | `+16.05%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `866491.887 ops/s` | `893678.659 ops/s` | `+3.14%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `848408.324 ops/s` | `874195.972 ops/s` | `+3.04%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18083.563 ops/s` | `19482.687 ops/s` | `+7.74%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6226.840 ops/s` | `7168.223 ops/s` | `+15.12%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6249.130 ops/s` | `7389.579 ops/s` | `+18.25%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2313.217 ops/s` | `3230.971 ops/s` | `+39.67%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2461.051 ops/s` | `3061.779 ops/s` | `+24.41%` | `better` |
| `segment-index-range-scan:boundedScan` | `32.244 us/op` | `30.293 us/op` | `-6.05%` | `warning` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2148.703 us/op` | `2179.621 us/op` | `+1.44%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4590.205 us/op` | `4244.928 us/op` | `-7.52%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `324.794 us/op` | `352.704 us/op` | `+8.59%` | `better` |
