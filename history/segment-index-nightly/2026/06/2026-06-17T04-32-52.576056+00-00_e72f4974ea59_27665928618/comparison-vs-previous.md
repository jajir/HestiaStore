# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `48.076 ops/s` | `46.345 ops/s` | `-3.60%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `42.265 ops/s` | `51.923 ops/s` | `+22.85%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `172630.973 ops/s` | `172242.159 ops/s` | `-0.23%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3808545.198 ops/s` | `3532207.076 ops/s` | `-7.26%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `90.798 ops/s` | `92.639 ops/s` | `+2.03%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `85.625 ops/s` | `81.851 ops/s` | `-4.41%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `176820.683 ops/s` | `170456.268 ops/s` | `-3.60%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3851355.615 ops/s` | `3567804.170 ops/s` | `-7.36%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160354.565 ops/s` | `135536.200 ops/s` | `-15.48%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `3753593.906 ops/s` | `4193273.628 ops/s` | `+11.71%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `166333.514 ops/s` | `165553.429 ops/s` | `-0.47%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3959238.208 ops/s` | `4093504.596 ops/s` | `+3.39%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `65048.937 ops/s` | `57742.641 ops/s` | `-11.23%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `116439.361 ops/s` | `112390.237 ops/s` | `-3.48%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `162219.844 ops/s` | `154232.334 ops/s` | `-4.92%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4001931.338 ops/s` | `3774149.805 ops/s` | `-5.69%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `2785688.485 ops/s` | `3030005.331 ops/s` | `+8.77%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1622753.130 ops/s` | `1710117.047 ops/s` | `+5.38%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.655 ms/op` | `246.636 ms/op` | `+0.40%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `264.605 ms/op` | `270.820 ms/op` | `+2.35%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `242.707 ms/op` | `245.407 ms/op` | `+1.11%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `509439.270 ops/s` | `506268.056 ops/s` | `-0.62%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `504103.140 ops/s` | `500918.426 ops/s` | `-0.63%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5336.130 ops/s` | `5349.629 ops/s` | `+0.25%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `273441.358 ops/s` | `300033.824 ops/s` | `+9.73%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `271896.567 ops/s` | `262908.478 ops/s` | `-3.31%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1544.792 ops/s` | `37125.346 ops/s` | `+2303.26%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2173.599 ops/s` | `2016.234 ops/s` | `-7.24%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2371.324 ops/s` | `2269.269 ops/s` | `-4.30%` | `warning` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2001.809 ops/s` | `2004.908 ops/s` | `+0.15%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2260.010 ops/s` | `2189.618 ops/s` | `-3.11%` | `warning` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8678918.985 ops/s` | `8514396.914 ops/s` | `-1.90%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7979965.392 ops/s` | `7903231.407 ops/s` | `-0.96%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9528113.624 ops/s` | `9460603.839 ops/s` | `-0.71%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7557163.862 ops/s` | `7453025.053 ops/s` | `-1.38%` | `neutral` |
