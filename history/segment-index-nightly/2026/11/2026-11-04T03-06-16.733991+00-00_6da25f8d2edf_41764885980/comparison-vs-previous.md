# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2930674.171 ops/s` | `6161971.545 ops/s` | `+110.26%` | `better` |
| `segment-index-get-live:getMissSync` | `2950328.807 ops/s` | `6628208.170 ops/s` | `+124.66%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `303431.750 ops/s` | `472475.307 ops/s` | `+55.71%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3334200.101 ops/s` | `6156385.998 ops/s` | `+84.64%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `1789016.166 ops/s` | `5366934.629 ops/s` | `+199.99%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2832598.311 ops/s` | `6053202.238 ops/s` | `+113.70%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1888652.021 ops/s` | `5469318.084 ops/s` | `+189.59%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2323798.264 ops/s` | `6254767.395 ops/s` | `+169.16%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2799197.783 ops/s` | `6041526.173 ops/s` | `+115.83%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1305031.693 ops/s` | `3011561.070 ops/s` | `+130.77%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `121.551 ms/op` | `199.489 ms/op` | `+64.12%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `137.245 ms/op` | `212.367 ms/op` | `+54.74%` | `better` |
| `segment-index-lifecycle:openExisting` | `119.544 ms/op` | `201.231 ms/op` | `+68.33%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `504791.554 ops/s` | `711290.135 ops/s` | `+40.91%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `238405.273 ops/s` | `428561.594 ops/s` | `+79.76%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `266386.281 ops/s` | `282728.541 ops/s` | `+6.13%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `938953.736 ops/s` | `1844550.732 ops/s` | `+96.45%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `900101.364 ops/s` | `1794139.829 ops/s` | `+99.33%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `38852.372 ops/s` | `50410.903 ops/s` | `+29.75%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `5567.861 ops/s` | `8347.901 ops/s` | `+49.93%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5846.319 ops/s` | `7922.478 ops/s` | `+35.51%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2547.728 ops/s` | `3835.987 ops/s` | `+50.57%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2523.527 ops/s` | `3672.365 ops/s` | `+45.53%` | `better` |
| `segment-index-range-scan:boundedScan` | `22.550 us/op` | `17.867 us/op` | `-20.77%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1837.318 us/op` | `1193.307 us/op` | `-35.05%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3508.877 us/op` | `2391.627 us/op` | `-31.84%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `308.378 us/op` | `187.769 us/op` | `-39.11%` | `worse` |
