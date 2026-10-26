# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6633943.905 ops/s` | `4806526.022 ops/s` | `-27.55%` | `worse` |
| `segment-index-get-live:getMissSync` | `5911291.560 ops/s` | `4473233.896 ops/s` | `-24.33%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `505335.557 ops/s` | `269689.648 ops/s` | `-46.63%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `6053958.755 ops/s` | `4707644.083 ops/s` | `-22.24%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `5044043.753 ops/s` | `3821987.180 ops/s` | `-24.23%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `6682645.662 ops/s` | `4679561.179 ops/s` | `-29.97%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `5216213.549 ops/s` | `3662673.277 ops/s` | `-29.78%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `5946399.807 ops/s` | `4522632.578 ops/s` | `-23.94%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `5511141.463 ops/s` | `4326579.626 ops/s` | `-21.49%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2999540.487 ops/s` | `2439008.932 ops/s` | `-18.69%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `218.219 ms/op` | `245.480 ms/op` | `+12.49%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `225.721 ms/op` | `261.088 ms/op` | `+15.67%` | `better` |
| `segment-index-lifecycle:openExisting` | `212.725 ms/op` | `241.717 ms/op` | `+13.63%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `674590.448 ops/s` | `548318.015 ops/s` | `-18.72%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `392496.988 ops/s` | `254518.204 ops/s` | `-35.15%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `282093.461 ops/s` | `293799.811 ops/s` | `+4.15%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1936556.427 ops/s` | `1214285.085 ops/s` | `-37.30%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1889265.899 ops/s` | `1180161.900 ops/s` | `-37.53%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `47290.528 ops/s` | `34123.185 ops/s` | `-27.84%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7875.728 ops/s` | `8863.270 ops/s` | `+12.54%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7760.894 ops/s` | `8927.083 ops/s` | `+15.03%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3591.626 ops/s` | `3272.956 ops/s` | `-8.87%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3469.387 ops/s` | `3220.781 ops/s` | `-7.17%` | `worse` |
| `segment-index-range-scan:boundedScan` | `21.426 us/op` | `30.280 us/op` | `+41.33%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1236.962 us/op` | `2006.414 us/op` | `+62.20%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2390.815 us/op` | `3758.578 us/op` | `+57.21%` | `better` |
| `segment-merge-sequential:mergeSequential` | `186.714 us/op` | `333.934 us/op` | `+78.85%` | `better` |
