# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `46.345 ops/s` | `48.456 ops/s` | `+4.56%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `51.923 ops/s` | `40.038 ops/s` | `-22.89%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `172242.159 ops/s` | `184203.007 ops/s` | `+6.94%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3532207.076 ops/s` | `3598265.685 ops/s` | `+1.87%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `92.639 ops/s` | `101.858 ops/s` | `+9.95%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `81.851 ops/s` | `93.597 ops/s` | `+14.35%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `170456.268 ops/s` | `185187.926 ops/s` | `+8.64%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3567804.170 ops/s` | `3552895.895 ops/s` | `-0.42%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `135536.200 ops/s` | `179080.727 ops/s` | `+32.13%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4193273.628 ops/s` | `3562276.997 ops/s` | `-15.05%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `165553.429 ops/s` | `182820.419 ops/s` | `+10.43%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4093504.596 ops/s` | `3685306.999 ops/s` | `-9.97%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `57742.641 ops/s` | `67174.892 ops/s` | `+16.33%` | `better` |
| `segment-index-get-persisted:getHitSync` | `112390.237 ops/s` | `113739.050 ops/s` | `+1.20%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `154232.334 ops/s` | `184518.961 ops/s` | `+19.64%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3774149.805 ops/s` | `3499792.523 ops/s` | `-7.27%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3030005.331 ops/s` | `2912775.815 ops/s` | `-3.87%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1710117.047 ops/s` | `1496305.765 ops/s` | `-12.50%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `246.636 ms/op` | `278.074 ms/op` | `+12.75%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `270.820 ms/op` | `303.148 ms/op` | `+11.94%` | `better` |
| `segment-index-lifecycle:openExisting` | `245.407 ms/op` | `275.545 ms/op` | `+12.28%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `506268.056 ops/s` | `520213.674 ops/s` | `+2.75%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `500918.426 ops/s` | `514837.580 ops/s` | `+2.78%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5349.629 ops/s` | `5376.094 ops/s` | `+0.49%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `300033.824 ops/s` | `289676.368 ops/s` | `-3.45%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `262908.478 ops/s` | `259171.680 ops/s` | `-1.42%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `37125.346 ops/s` | `30504.688 ops/s` | `-17.83%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2016.234 ops/s` | `2504.380 ops/s` | `+24.21%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2269.269 ops/s` | `2744.437 ops/s` | `+20.94%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2004.908 ops/s` | `2490.310 ops/s` | `+24.21%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2189.618 ops/s` | `2752.567 ops/s` | `+25.71%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8514396.914 ops/s` | `8308783.480 ops/s` | `-2.41%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7903231.407 ops/s` | `7584628.289 ops/s` | `-4.03%` | `warning` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9460603.839 ops/s` | `8582665.804 ops/s` | `-9.28%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `7453025.053 ops/s` | `6718807.014 ops/s` | `-9.85%` | `worse` |
