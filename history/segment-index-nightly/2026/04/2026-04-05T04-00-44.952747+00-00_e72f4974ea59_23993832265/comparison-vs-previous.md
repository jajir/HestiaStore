# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `45.069 ops/s` | `46.999 ops/s` | `+4.28%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `39.608 ops/s` | `51.892 ops/s` | `+31.02%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `189537.165 ops/s` | `173625.546 ops/s` | `-8.39%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `3754196.106 ops/s` | `3757845.033 ops/s` | `+0.10%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `92.576 ops/s` | `88.137 ops/s` | `-4.79%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `106.489 ops/s` | `81.539 ops/s` | `-23.43%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `188781.742 ops/s` | `174090.688 ops/s` | `-7.78%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3764415.098 ops/s` | `3599783.794 ops/s` | `-4.37%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `179024.352 ops/s` | `166343.451 ops/s` | `-7.08%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `3640703.569 ops/s` | `3761976.887 ops/s` | `+3.33%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `179927.488 ops/s` | `159320.947 ops/s` | `-11.45%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `4207040.470 ops/s` | `4064048.295 ops/s` | `-3.40%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `66951.518 ops/s` | `64785.488 ops/s` | `-3.24%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `111334.331 ops/s` | `111726.134 ops/s` | `+0.35%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `180884.067 ops/s` | `166904.343 ops/s` | `-7.73%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3851385.830 ops/s` | `4364538.962 ops/s` | `+13.32%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2465427.950 ops/s` | `3382076.732 ops/s` | `+37.18%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1715523.491 ops/s` | `1669005.659 ops/s` | `-2.71%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.591 ms/op` | `244.429 ms/op` | `-0.07%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `266.573 ms/op` | `266.992 ms/op` | `+0.16%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `243.920 ms/op` | `240.608 ms/op` | `-1.36%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `491663.428 ops/s` | `516290.416 ops/s` | `+5.01%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `486322.234 ops/s` | `510993.170 ops/s` | `+5.07%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5341.194 ops/s` | `5297.246 ops/s` | `-0.82%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `269855.446 ops/s` | `264166.275 ops/s` | `-2.11%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `268376.359 ops/s` | `262590.201 ops/s` | `-2.16%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1479.087 ops/s` | `1576.075 ops/s` | `+6.56%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1369.091 ops/s` | `1996.984 ops/s` | `+45.86%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1401.783 ops/s` | `2153.552 ops/s` | `+53.63%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1445.853 ops/s` | `1928.439 ops/s` | `+33.38%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1438.897 ops/s` | `2172.662 ops/s` | `+50.99%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8536381.113 ops/s` | `8511133.331 ops/s` | `-0.30%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7874846.368 ops/s` | `7950112.736 ops/s` | `+0.96%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8701852.195 ops/s` | `9056191.936 ops/s` | `+4.07%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `7310851.102 ops/s` | `7571941.212 ops/s` | `+3.57%` | `better` |
