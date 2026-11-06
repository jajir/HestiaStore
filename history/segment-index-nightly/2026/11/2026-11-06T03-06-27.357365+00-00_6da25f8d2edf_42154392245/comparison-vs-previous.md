# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6128689.633 ops/s` | `6683141.987 ops/s` | `+9.05%` | `better` |
| `segment-index-get-live:getMissSync` | `7053667.842 ops/s` | `6465330.229 ops/s` | `-8.34%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `482166.264 ops/s` | `469017.052 ops/s` | `-2.73%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `6431711.887 ops/s` | `6618028.574 ops/s` | `+2.90%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `5511923.762 ops/s` | `5788354.400 ops/s` | `+5.02%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `6668949.351 ops/s` | `7129748.325 ops/s` | `+6.91%` | `better` |
| `segment-index-get-persisted:getHitSync` | `5289022.643 ops/s` | `5663790.322 ops/s` | `+7.09%` | `better` |
| `segment-index-get-persisted:getMissSync` | `6349007.725 ops/s` | `6177605.840 ops/s` | `-2.70%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `5868262.197 ops/s` | `6449914.396 ops/s` | `+9.91%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `3098351.654 ops/s` | `3459643.992 ops/s` | `+11.66%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `182.910 ms/op` | `179.289 ms/op` | `-1.98%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `203.219 ms/op` | `191.624 ms/op` | `-5.71%` | `warning` |
| `segment-index-lifecycle:openExisting` | `186.876 ms/op` | `181.129 ms/op` | `-3.08%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `680991.095 ops/s` | `723722.508 ops/s` | `+6.27%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `395267.383 ops/s` | `433438.604 ops/s` | `+9.66%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `285723.712 ops/s` | `290283.905 ops/s` | `+1.60%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1909831.102 ops/s` | `1855211.063 ops/s` | `-2.86%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1859610.447 ops/s` | `1803956.143 ops/s` | `-2.99%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `50220.655 ops/s` | `51254.921 ops/s` | `+2.06%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8410.637 ops/s` | `8135.445 ops/s` | `-3.27%` | `warning` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8212.463 ops/s` | `8245.170 ops/s` | `+0.40%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3841.813 ops/s` | `3001.111 ops/s` | `-21.88%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3647.113 ops/s` | `3906.735 ops/s` | `+7.12%` | `better` |
| `segment-index-range-scan:boundedScan` | `16.926 us/op` | `18.004 us/op` | `+6.37%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1174.051 us/op` | `1164.281 us/op` | `-0.83%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `2288.007 us/op` | `2257.479 us/op` | `-1.33%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `181.359 us/op` | `173.867 us/op` | `-4.13%` | `warning` |
