# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-mixed-split-heavy`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `45.523 ops/s` | `41.463 ops/s` | `-8.92%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `38.331 ops/s` | `48.185 ops/s` | `+25.71%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `237176.262 ops/s` | `175238.433 ops/s` | `-26.11%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `4455206.167 ops/s` | `3901727.445 ops/s` | `-12.42%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `95.164 ops/s` | `101.419 ops/s` | `+6.57%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `91.928 ops/s` | `95.302 ops/s` | `+3.67%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `237033.228 ops/s` | `173634.006 ops/s` | `-26.75%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4607050.016 ops/s` | `3790990.815 ops/s` | `-17.71%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `235430.194 ops/s` | `166443.513 ops/s` | `-29.30%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `4746913.067 ops/s` | `4152178.532 ops/s` | `-12.53%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `237329.359 ops/s` | `169262.324 ops/s` | `-28.68%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `4400838.361 ops/s` | `4428039.527 ops/s` | `+0.62%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `83639.344 ops/s` | `61024.361 ops/s` | `-27.04%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `159670.755 ops/s` | `118513.450 ops/s` | `-25.78%` | `worse` |
| `segment-index-get-persisted:getMissAsyncJoin` | `233403.219 ops/s` | `164648.594 ops/s` | `-29.46%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4583833.967 ops/s` | `3668708.157 ops/s` | `-19.96%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3612721.425 ops/s` | `3177279.925 ops/s` | `-12.05%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1940375.911 ops/s` | `1618454.573 ops/s` | `-16.59%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `219.046 ms/op` | `247.410 ms/op` | `+12.95%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `234.353 ms/op` | `267.961 ms/op` | `+14.34%` | `better` |
| `segment-index-lifecycle:openExisting` | `214.754 ms/op` | `244.785 ms/op` | `+13.98%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `749793.731 ops/s` | `512491.868 ops/s` | `-31.65%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `744472.114 ops/s` | `507177.522 ops/s` | `-31.87%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5321.617 ops/s` | `5314.346 ops/s` | `-0.14%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `380158.321 ops/s` | `272648.760 ops/s` | `-28.28%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `374757.533 ops/s` | `256785.544 ops/s` | `-31.48%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5400.788 ops/s` | `1516.842 ops/s` | `-71.91%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1787.623 ops/s` | `2060.168 ops/s` | `+15.25%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1699.129 ops/s` | `2293.262 ops/s` | `+34.97%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1388.631 ops/s` | `2010.847 ops/s` | `+44.81%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1788.448 ops/s` | `2248.926 ops/s` | `+25.75%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `11109309.985 ops/s` | `8573825.565 ops/s` | `-22.82%` | `worse` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `10073509.428 ops/s` | `7890789.325 ops/s` | `-21.67%` | `worse` |
| `sorted-data-diff-key-read-compact:readNextKey` | `10512266.355 ops/s` | `8841772.508 ops/s` | `-15.89%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `8547034.936 ops/s` | `7469610.391 ops/s` | `-12.61%` | `worse` |
