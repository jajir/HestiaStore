# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6683141.987 ops/s` | `4847025.080 ops/s` | `-27.47%` | `worse` |
| `segment-index-get-live:getMissSync` | `6465330.229 ops/s` | `3941599.347 ops/s` | `-39.03%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `469017.052 ops/s` | `290134.092 ops/s` | `-38.14%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `6618028.574 ops/s` | `4390162.768 ops/s` | `-33.66%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `5788354.400 ops/s` | `3247766.866 ops/s` | `-43.89%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `7129748.325 ops/s` | `4631304.020 ops/s` | `-35.04%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `5663790.322 ops/s` | `3299638.486 ops/s` | `-41.74%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `6177605.840 ops/s` | `4471064.413 ops/s` | `-27.62%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `6449914.396 ops/s` | `3766008.623 ops/s` | `-41.61%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `3459643.992 ops/s` | `2173201.877 ops/s` | `-37.18%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `179.289 ms/op` | `225.289 ms/op` | `+25.66%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `191.624 ms/op` | `240.122 ms/op` | `+25.31%` | `better` |
| `segment-index-lifecycle:openExisting` | `181.129 ms/op` | `220.045 ms/op` | `+21.49%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `723722.508 ops/s` | `545591.009 ops/s` | `-24.61%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `433438.604 ops/s` | `256113.348 ops/s` | `-40.91%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `290283.905 ops/s` | `289477.661 ops/s` | `-0.28%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1855211.063 ops/s` | `1134407.799 ops/s` | `-38.85%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1803956.143 ops/s` | `1099121.819 ops/s` | `-39.07%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `51254.921 ops/s` | `35285.980 ops/s` | `-31.16%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8135.445 ops/s` | `7801.964 ops/s` | `-4.10%` | `warning` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8245.170 ops/s` | `7805.379 ops/s` | `-5.33%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `3001.111 ops/s` | `3574.286 ops/s` | `+19.10%` | `better` |
| `segment-index-persisted-mutation:putSync` | `3906.735 ops/s` | `3434.566 ops/s` | `-12.09%` | `worse` |
| `segment-index-range-scan:boundedScan` | `18.004 us/op` | `27.250 us/op` | `+51.35%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1164.281 us/op` | `2027.723 us/op` | `+74.16%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2257.479 us/op` | `3832.334 us/op` | `+69.76%` | `better` |
| `segment-merge-sequential:mergeSequential` | `173.867 us/op` | `336.393 us/op` | `+93.48%` | `better` |
