# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `bf830eb6be92f45201d79a8df2c45b2570dec148`
- Candidate SHA: `0729d192cbf197172df20d08a0853a441bde7891`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4930000.319 ops/s` | `4962622.404 ops/s` | `+0.66%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4863914.805 ops/s` | `4710283.758 ops/s` | `-3.16%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3338864.105 ops/s` | `3214450.495 ops/s` | `-3.73%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4553286.052 ops/s` | `4810402.341 ops/s` | `+5.65%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3300569.917 ops/s` | `3189831.769 ops/s` | `-3.36%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4687755.521 ops/s` | `4503482.261 ops/s` | `-3.93%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `4275082.908 ops/s` | `4429207.491 ops/s` | `+3.61%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2179288.776 ops/s` | `2202210.876 ops/s` | `+1.05%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `600171.571 ops/s` | `586617.479 ops/s` | `-2.26%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `426096.053 ops/s` | `396582.575 ops/s` | `-6.93%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `174075.517 ops/s` | `190034.904 ops/s` | `+9.17%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `866491.887 ops/s` | `805715.784 ops/s` | `-7.01%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `848408.324 ops/s` | `784806.313 ops/s` | `-7.50%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18083.563 ops/s` | `20909.472 ops/s` | `+15.63%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6226.840 ops/s` | `6490.208 ops/s` | `+4.23%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6249.130 ops/s` | `6431.254 ops/s` | `+2.91%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2313.217 ops/s` | `2592.083 ops/s` | `+12.06%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2461.051 ops/s` | `2577.585 ops/s` | `+4.74%` | `better` |
| `segment-index-range-scan:boundedScan` | `32.244 us/op` | `29.862 us/op` | `-7.39%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2148.703 us/op` | `2075.677 us/op` | `-3.40%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `4590.205 us/op` | `4034.419 us/op` | `-12.11%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `324.794 us/op` | `324.869 us/op` | `+0.02%` | `neutral` |
