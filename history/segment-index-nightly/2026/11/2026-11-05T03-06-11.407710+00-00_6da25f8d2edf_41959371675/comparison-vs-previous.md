# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6161971.545 ops/s` | `6128689.633 ops/s` | `-0.54%` | `neutral` |
| `segment-index-get-live:getMissSync` | `6628208.170 ops/s` | `7053667.842 ops/s` | `+6.42%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `472475.307 ops/s` | `482166.264 ops/s` | `+2.05%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `6156385.998 ops/s` | `6431711.887 ops/s` | `+4.47%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `5366934.629 ops/s` | `5511923.762 ops/s` | `+2.70%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `6053202.238 ops/s` | `6668949.351 ops/s` | `+10.17%` | `better` |
| `segment-index-get-persisted:getHitSync` | `5469318.084 ops/s` | `5289022.643 ops/s` | `-3.30%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `6254767.395 ops/s` | `6349007.725 ops/s` | `+1.51%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `6041526.173 ops/s` | `5868262.197 ops/s` | `-2.87%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `3011561.070 ops/s` | `3098351.654 ops/s` | `+2.88%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `199.489 ms/op` | `182.910 ms/op` | `-8.31%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `212.367 ms/op` | `203.219 ms/op` | `-4.31%` | `warning` |
| `segment-index-lifecycle:openExisting` | `201.231 ms/op` | `186.876 ms/op` | `-7.13%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `711290.135 ops/s` | `680991.095 ops/s` | `-4.26%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `428561.594 ops/s` | `395267.383 ops/s` | `-7.77%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `282728.541 ops/s` | `285723.712 ops/s` | `+1.06%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1844550.732 ops/s` | `1909831.102 ops/s` | `+3.54%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1794139.829 ops/s` | `1859610.447 ops/s` | `+3.65%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `50410.903 ops/s` | `50220.655 ops/s` | `-0.38%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8347.901 ops/s` | `8410.637 ops/s` | `+0.75%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7922.478 ops/s` | `8212.463 ops/s` | `+3.66%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3835.987 ops/s` | `3841.813 ops/s` | `+0.15%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3672.365 ops/s` | `3647.113 ops/s` | `-0.69%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `17.867 us/op` | `16.926 us/op` | `-5.27%` | `warning` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1193.307 us/op` | `1174.051 us/op` | `-1.61%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `2391.627 us/op` | `2288.007 us/op` | `-4.33%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `187.769 us/op` | `181.359 us/op` | `-3.41%` | `warning` |
