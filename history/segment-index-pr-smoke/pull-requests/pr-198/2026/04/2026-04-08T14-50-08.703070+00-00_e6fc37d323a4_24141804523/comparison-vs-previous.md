# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e6fc37d323a4c06b23c345b47bdc82a75aa07aca`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.043 ops/s` | `100.314 ops/s` | `+3.37%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.004 ops/s` | `89.709 ops/s` | `+6.79%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165098.087 ops/s` | `186131.463 ops/s` | `+12.74%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3826025.739 ops/s` | `4083732.585 ops/s` | `+6.74%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `155664.746 ops/s` | `166731.756 ops/s` | `+7.11%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4213435.612 ops/s` | `4067421.649 ops/s` | `-3.47%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156455.503 ops/s` | `167409.279 ops/s` | `+7.00%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4124415.193 ops/s` | `3851182.545 ops/s` | `-6.62%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55789.753 ops/s` | `61120.055 ops/s` | `+9.55%` | `better` |
| `segment-index-get-persisted:getHitSync` | `104396.145 ops/s` | `119621.051 ops/s` | `+14.58%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165994.696 ops/s` | `169898.153 ops/s` | `+2.35%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3940414.940 ops/s` | `4159352.446 ops/s` | `+5.56%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3124721.140 ops/s` | `3093024.764 ops/s` | `-1.01%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1717560.089 ops/s` | `1560900.558 ops/s` | `-9.12%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `455608.507 ops/s` | `385749.613 ops/s` | `-15.33%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450389.833 ops/s` | `380567.852 ops/s` | `-15.50%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5218.674 ops/s` | `5181.761 ops/s` | `-0.71%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `207684.917 ops/s` | `272581.793 ops/s` | `+31.25%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `205044.275 ops/s` | `192200.593 ops/s` | `-6.26%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2640.642 ops/s` | `80381.200 ops/s` | `+2944.00%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2160.231 ops/s` | `2223.541 ops/s` | `+2.93%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `54491.914 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2479.974 ops/s` | `2524.883 ops/s` | `+1.81%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `58277.041 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `3.332 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2215.411 ops/s` | `2235.095 ops/s` | `+0.89%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230876.647 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2514.868 ops/s` | `2485.518 ops/s` | `-1.17%` | `neutral` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `258610.600 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.982 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7932313.730 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6487103.344 ops/s` | `-` | `-` | `removed` |
