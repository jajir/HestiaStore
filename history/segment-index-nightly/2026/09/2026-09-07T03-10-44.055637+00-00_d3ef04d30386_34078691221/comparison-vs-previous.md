# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4744117.365 ops/s` | `5036323.727 ops/s` | `+6.16%` | `better` |
| `segment-index-get-live:getMissSync` | `4332772.948 ops/s` | `4829862.423 ops/s` | `+11.47%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `281119.086 ops/s` | `269733.144 ops/s` | `-4.05%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `4798410.858 ops/s` | `4555348.571 ops/s` | `-5.07%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3470591.822 ops/s` | `3626580.555 ops/s` | `+4.49%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3999458.579 ops/s` | `4655959.924 ops/s` | `+16.41%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3389699.220 ops/s` | `3660990.711 ops/s` | `+8.00%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4495156.191 ops/s` | `4840660.467 ops/s` | `+7.69%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4148394.927 ops/s` | `4596305.357 ops/s` | `+10.80%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2181783.466 ops/s` | `2163894.106 ops/s` | `-0.82%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `274.983 ms/op` | `243.100 ms/op` | `-11.59%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `298.517 ms/op` | `266.211 ms/op` | `-10.82%` | `worse` |
| `segment-index-lifecycle:openExisting` | `274.824 ms/op` | `239.933 ms/op` | `-12.70%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `551223.337 ops/s` | `547792.832 ops/s` | `-0.62%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `270246.759 ops/s` | `286310.529 ops/s` | `+5.94%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `280976.578 ops/s` | `261482.303 ops/s` | `-6.94%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1226758.693 ops/s` | `1259258.384 ops/s` | `+2.65%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1204749.913 ops/s` | `1238554.366 ops/s` | `+2.81%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `22008.780 ops/s` | `20704.018 ops/s` | `-5.93%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7638.271 ops/s` | `7122.232 ops/s` | `-6.76%` | `warning` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8087.589 ops/s` | `7063.643 ops/s` | `-12.66%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3181.132 ops/s` | `2455.662 ops/s` | `-22.81%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3024.729 ops/s` | `2391.827 ops/s` | `-20.92%` | `worse` |
| `segment-index-range-scan:boundedScan` | `28.665 us/op` | `29.220 us/op` | `+1.94%` | `neutral` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2094.091 us/op` | `2006.538 us/op` | `-4.18%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `4092.022 us/op` | `3934.287 us/op` | `-3.85%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `359.195 us/op` | `331.984 us/op` | `-7.58%` | `worse` |
