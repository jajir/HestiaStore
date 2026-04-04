# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-persisted-mutation`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `54.522 ops/s` | `45.069 ops/s` | `-17.34%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `56.570 ops/s` | `39.608 ops/s` | `-29.99%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `170547.616 ops/s` | `189537.165 ops/s` | `+11.13%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4028489.029 ops/s` | `3754196.106 ops/s` | `-6.81%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `94.350 ops/s` | `92.576 ops/s` | `-1.88%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `88.830 ops/s` | `106.489 ops/s` | `+19.88%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `170848.708 ops/s` | `188781.742 ops/s` | `+10.50%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3530093.457 ops/s` | `3764415.098 ops/s` | `+6.64%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `166171.322 ops/s` | `179024.352 ops/s` | `+7.73%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4006278.896 ops/s` | `3640703.569 ops/s` | `-9.13%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `162818.997 ops/s` | `179927.488 ops/s` | `+10.51%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3613159.040 ops/s` | `4207040.470 ops/s` | `+16.44%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `65752.932 ops/s` | `66951.518 ops/s` | `+1.82%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `116849.407 ops/s` | `111334.331 ops/s` | `-4.72%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `156722.374 ops/s` | `180884.067 ops/s` | `+15.42%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4043149.258 ops/s` | `3851385.830 ops/s` | `-4.74%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `3126026.231 ops/s` | `2465427.950 ops/s` | `-21.13%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1765939.775 ops/s` | `1715523.491 ops/s` | `-2.85%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `246.250 ms/op` | `244.591 ms/op` | `-0.67%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `267.023 ms/op` | `266.573 ms/op` | `-0.17%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `240.587 ms/op` | `243.920 ms/op` | `+1.39%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `508138.538 ops/s` | `491663.428 ops/s` | `-3.24%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `502792.857 ops/s` | `486322.234 ops/s` | `-3.28%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5345.681 ops/s` | `5341.194 ops/s` | `-0.08%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `266948.957 ops/s` | `269855.446 ops/s` | `+1.09%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `265553.481 ops/s` | `268376.359 ops/s` | `+1.06%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1395.476 ops/s` | `1479.087 ops/s` | `+5.99%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2076.296 ops/s` | `1369.091 ops/s` | `-34.06%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2261.685 ops/s` | `1401.783 ops/s` | `-38.02%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2005.416 ops/s` | `1445.853 ops/s` | `-27.90%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2202.117 ops/s` | `1438.897 ops/s` | `-34.66%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8442677.732 ops/s` | `8536381.113 ops/s` | `+1.11%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7528829.686 ops/s` | `7874846.368 ops/s` | `+4.60%` | `better` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8572850.211 ops/s` | `8701852.195 ops/s` | `+1.50%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7523282.181 ops/s` | `7310851.102 ops/s` | `-2.82%` | `neutral` |
