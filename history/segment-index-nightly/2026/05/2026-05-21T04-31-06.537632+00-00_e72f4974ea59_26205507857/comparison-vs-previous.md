# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot,segment-index-lifecycle,segment-index-persisted-mutation`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `51.649 ops/s` | `45.523 ops/s` | `-11.86%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `45.401 ops/s` | `38.331 ops/s` | `-15.57%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `184287.486 ops/s` | `237176.262 ops/s` | `+28.70%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3425006.672 ops/s` | `4455206.167 ops/s` | `+30.08%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `104.043 ops/s` | `95.164 ops/s` | `-8.53%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `96.648 ops/s` | `91.928 ops/s` | `-4.88%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `187838.719 ops/s` | `237033.228 ops/s` | `+26.19%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3620977.616 ops/s` | `4607050.016 ops/s` | `+27.23%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `179651.815 ops/s` | `235430.194 ops/s` | `+31.05%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3767983.389 ops/s` | `4746913.067 ops/s` | `+25.98%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `179155.860 ops/s` | `237329.359 ops/s` | `+32.47%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3696270.936 ops/s` | `4400838.361 ops/s` | `+19.06%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `65158.102 ops/s` | `83639.344 ops/s` | `+28.36%` | `better` |
| `segment-index-get-persisted:getHitSync` | `113094.388 ops/s` | `159670.755 ops/s` | `+41.18%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `181603.634 ops/s` | `233403.219 ops/s` | `+28.52%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3267435.525 ops/s` | `4583833.967 ops/s` | `+40.29%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2664618.750 ops/s` | `3612721.425 ops/s` | `+35.58%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1289564.624 ops/s` | `1940375.911 ops/s` | `+50.47%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `279.386 ms/op` | `219.046 ms/op` | `-21.60%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `302.347 ms/op` | `234.353 ms/op` | `-22.49%` | `worse` |
| `segment-index-lifecycle:openExisting` | `274.244 ms/op` | `214.754 ms/op` | `-21.69%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `515573.479 ops/s` | `749793.731 ops/s` | `+45.43%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `510241.923 ops/s` | `744472.114 ops/s` | `+45.91%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5331.556 ops/s` | `5321.617 ops/s` | `-0.19%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `302149.623 ops/s` | `380158.321 ops/s` | `+25.82%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `266852.855 ops/s` | `374757.533 ops/s` | `+40.44%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `35296.767 ops/s` | `5400.788 ops/s` | `-84.70%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2436.703 ops/s` | `1787.623 ops/s` | `-26.64%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2749.420 ops/s` | `1699.129 ops/s` | `-38.20%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2410.617 ops/s` | `1388.631 ops/s` | `-42.40%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2594.774 ops/s` | `1788.448 ops/s` | `-31.07%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8261790.654 ops/s` | `11109309.985 ops/s` | `+34.47%` | `better` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7550676.595 ops/s` | `10073509.428 ops/s` | `+33.41%` | `better` |
| `sorted-data-diff-key-read-compact:readNextKey` | `7802745.668 ops/s` | `10512266.355 ops/s` | `+34.73%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `6752247.114 ops/s` | `8547034.936 ops/s` | `+26.58%` | `better` |
