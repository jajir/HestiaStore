# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `f2b2b52ff47e7b0a2c4e5b7f9c75e21855610b56`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.043 ops/s` | `84.075 ops/s` | `-13.36%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.004 ops/s` | `82.238 ops/s` | `-2.10%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165098.087 ops/s` | `184250.626 ops/s` | `+11.60%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3826025.739 ops/s` | `3878946.558 ops/s` | `+1.38%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `155664.746 ops/s` | `181587.633 ops/s` | `+16.65%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4213435.612 ops/s` | `4080843.626 ops/s` | `-3.15%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156455.503 ops/s` | `186885.012 ops/s` | `+19.45%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4124415.193 ops/s` | `3581803.885 ops/s` | `-13.16%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55789.753 ops/s` | `58030.843 ops/s` | `+4.02%` | `better` |
| `segment-index-get-persisted:getHitSync` | `104396.145 ops/s` | `108815.436 ops/s` | `+4.23%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165994.696 ops/s` | `182655.479 ops/s` | `+10.04%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3940414.940 ops/s` | `3663450.980 ops/s` | `-7.03%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3124721.140 ops/s` | `2803869.445 ops/s` | `-10.27%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1717560.089 ops/s` | `1567287.105 ops/s` | `-8.75%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `455608.507 ops/s` | `416793.569 ops/s` | `-8.52%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450389.833 ops/s` | `411859.739 ops/s` | `-8.55%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5218.674 ops/s` | `4933.830 ops/s` | `-5.46%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `207684.917 ops/s` | `197274.900 ops/s` | `-5.01%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `205044.275 ops/s` | `194861.455 ops/s` | `-4.97%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2640.642 ops/s` | `2413.445 ops/s` | `-8.60%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2160.231 ops/s` | `2657.052 ops/s` | `+23.00%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `54491.914 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2479.974 ops/s` | `3201.343 ops/s` | `+29.09%` | `better` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `58277.041 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `3.332 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2215.411 ops/s` | `2746.597 ops/s` | `+23.98%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230876.647 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2514.868 ops/s` | `3076.023 ops/s` | `+22.31%` | `better` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `258610.600 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.982 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7932313.730 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6487103.344 ops/s` | `-` | `-` | `removed` |
