# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-mixed-split-heavy`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `48.456 ops/s` | `58.143 ops/s` | `+19.99%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `40.038 ops/s` | `42.522 ops/s` | `+6.20%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `184203.007 ops/s` | `168820.490 ops/s` | `-8.35%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `3598265.685 ops/s` | `3448713.765 ops/s` | `-4.16%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `101.858 ops/s` | `100.889 ops/s` | `-0.95%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `93.597 ops/s` | `91.643 ops/s` | `-2.09%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `185187.926 ops/s` | `167671.969 ops/s` | `-9.46%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3552895.895 ops/s` | `3553518.088 ops/s` | `+0.02%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `179080.727 ops/s` | `160264.953 ops/s` | `-10.51%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `3562276.997 ops/s` | `3815169.024 ops/s` | `+7.10%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `182820.419 ops/s` | `153510.613 ops/s` | `-16.03%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `3685306.999 ops/s` | `3993313.003 ops/s` | `+8.36%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `67174.892 ops/s` | `58180.563 ops/s` | `-13.39%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `113739.050 ops/s` | `112759.918 ops/s` | `-0.86%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `184518.961 ops/s` | `154262.166 ops/s` | `-16.40%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3499792.523 ops/s` | `3339067.632 ops/s` | `-4.59%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `2912775.815 ops/s` | `2982169.444 ops/s` | `+2.38%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1496305.765 ops/s` | `1552105.380 ops/s` | `+3.73%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.074 ms/op` | `250.116 ms/op` | `-10.05%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `303.148 ms/op` | `269.981 ms/op` | `-10.94%` | `worse` |
| `segment-index-lifecycle:openExisting` | `275.545 ms/op` | `248.998 ms/op` | `-9.63%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `520213.674 ops/s` | `517152.591 ops/s` | `-0.59%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `514837.580 ops/s` | `511805.626 ops/s` | `-0.59%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5376.094 ops/s` | `5346.965 ops/s` | `-0.54%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `289676.368 ops/s` | `258415.663 ops/s` | `-10.79%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `259171.680 ops/s` | `257012.946 ops/s` | `-0.83%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `30504.688 ops/s` | `1443.146 ops/s` | `-95.27%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2504.380 ops/s` | `2041.702 ops/s` | `-18.47%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2744.437 ops/s` | `2269.176 ops/s` | `-17.32%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2490.310 ops/s` | `2011.274 ops/s` | `-19.24%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2752.567 ops/s` | `2197.006 ops/s` | `-20.18%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8308783.480 ops/s` | `8301418.955 ops/s` | `-0.09%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7584628.289 ops/s` | `7885065.550 ops/s` | `+3.96%` | `better` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8582665.804 ops/s` | `8921693.355 ops/s` | `+3.95%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `6718807.014 ops/s` | `7220162.076 ops/s` | `+7.46%` | `better` |
