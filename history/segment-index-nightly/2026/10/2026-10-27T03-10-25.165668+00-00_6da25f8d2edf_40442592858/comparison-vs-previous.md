# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4806526.022 ops/s` | `7216495.081 ops/s` | `+50.14%` | `better` |
| `segment-index-get-live:getMissSync` | `4473233.896 ops/s` | `7033229.690 ops/s` | `+57.23%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `269689.648 ops/s` | `494576.984 ops/s` | `+83.39%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4707644.083 ops/s` | `6743573.710 ops/s` | `+43.25%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3821987.180 ops/s` | `5783745.246 ops/s` | `+51.33%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4679561.179 ops/s` | `6447251.017 ops/s` | `+37.77%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3662673.277 ops/s` | `5468522.907 ops/s` | `+49.30%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4522632.578 ops/s` | `6237649.380 ops/s` | `+37.92%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4326579.626 ops/s` | `6305781.880 ops/s` | `+45.75%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2439008.932 ops/s` | `3320651.032 ops/s` | `+36.15%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.480 ms/op` | `205.408 ms/op` | `-16.32%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `261.088 ms/op` | `217.612 ms/op` | `-16.65%` | `worse` |
| `segment-index-lifecycle:openExisting` | `241.717 ms/op` | `198.780 ms/op` | `-17.76%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `548318.015 ops/s` | `724960.633 ops/s` | `+32.22%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `254518.204 ops/s` | `439306.599 ops/s` | `+72.60%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `293799.811 ops/s` | `285654.034 ops/s` | `-2.77%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1214285.085 ops/s` | `2011286.607 ops/s` | `+65.64%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1180161.900 ops/s` | `1960395.419 ops/s` | `+66.11%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `34123.185 ops/s` | `50891.189 ops/s` | `+49.14%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8863.270 ops/s` | `8375.870 ops/s` | `-5.50%` | `warning` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8927.083 ops/s` | `7903.515 ops/s` | `-11.47%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3272.956 ops/s` | `3616.737 ops/s` | `+10.50%` | `better` |
| `segment-index-persisted-mutation:putSync` | `3220.781 ops/s` | `3647.453 ops/s` | `+13.25%` | `better` |
| `segment-index-range-scan:boundedScan` | `30.280 us/op` | `20.010 us/op` | `-33.92%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2006.414 us/op` | `1205.453 us/op` | `-39.92%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3758.578 us/op` | `2338.313 us/op` | `-37.79%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `333.934 us/op` | `184.189 us/op` | `-44.84%` | `worse` |
