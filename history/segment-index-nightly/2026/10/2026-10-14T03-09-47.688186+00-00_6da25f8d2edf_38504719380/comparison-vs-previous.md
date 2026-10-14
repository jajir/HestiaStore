# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5229017.109 ops/s` | `3241540.775 ops/s` | `-38.01%` | `worse` |
| `segment-index-get-live:getMissSync` | `4273867.324 ops/s` | `3278716.188 ops/s` | `-23.28%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `274204.854 ops/s` | `270690.594 ops/s` | `-1.28%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4843691.667 ops/s` | `3373574.993 ops/s` | `-30.35%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3482692.078 ops/s` | `2551890.339 ops/s` | `-26.73%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4400023.194 ops/s` | `2805403.407 ops/s` | `-36.24%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3385451.409 ops/s` | `2217430.745 ops/s` | `-34.50%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4365539.568 ops/s` | `3132070.061 ops/s` | `-28.25%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4502999.033 ops/s` | `2918592.872 ops/s` | `-35.19%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2116285.121 ops/s` | `1678980.265 ops/s` | `-20.66%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.258 ms/op` | `133.140 ms/op` | `-45.71%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `262.307 ms/op` | `149.924 ms/op` | `-42.84%` | `worse` |
| `segment-index-lifecycle:openExisting` | `242.346 ms/op` | `130.572 ms/op` | `-46.12%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `550946.627 ops/s` | `483958.130 ops/s` | `-12.16%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `251487.274 ops/s` | `225659.424 ops/s` | `-10.27%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `299459.353 ops/s` | `258298.706 ops/s` | `-13.74%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1215735.771 ops/s` | `847238.633 ops/s` | `-30.31%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1179226.040 ops/s` | `808881.870 ops/s` | `-31.41%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `36509.730 ops/s` | `38356.763 ops/s` | `+5.06%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7953.093 ops/s` | `10553.996 ops/s` | `+32.70%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8300.425 ops/s` | `10620.577 ops/s` | `+27.95%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3042.575 ops/s` | `4330.506 ops/s` | `+42.33%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2869.352 ops/s` | `4203.195 ops/s` | `+46.49%` | `better` |
| `segment-index-range-scan:boundedScan` | `31.807 us/op` | `28.251 us/op` | `-11.18%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2001.486 us/op` | `2036.127 us/op` | `+1.73%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3910.248 us/op` | `3865.441 us/op` | `-1.15%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `334.348 us/op` | `331.972 us/op` | `-0.71%` | `neutral` |
