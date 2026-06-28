# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.101 ops/s` | `46.048 ops/s` | `+12.04%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `42.263 ops/s` | `44.627 ops/s` | `+5.60%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `186614.783 ops/s` | `184578.724 ops/s` | `-1.09%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3383906.683 ops/s` | `3652743.000 ops/s` | `+7.94%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `91.049 ops/s` | `100.428 ops/s` | `+10.30%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `89.758 ops/s` | `105.653 ops/s` | `+17.71%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `185879.417 ops/s` | `184366.002 ops/s` | `-0.81%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3736066.788 ops/s` | `3577133.221 ops/s` | `-4.25%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `185847.187 ops/s` | `182653.643 ops/s` | `-1.72%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3689396.334 ops/s` | `3367967.483 ops/s` | `-8.71%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `179723.296 ops/s` | `182661.646 ops/s` | `+1.63%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3186486.560 ops/s` | `3514883.136 ops/s` | `+10.31%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `62371.621 ops/s` | `67675.328 ops/s` | `+8.50%` | `better` |
| `segment-index-get-persisted:getHitSync` | `115858.945 ops/s` | `110159.849 ops/s` | `-4.92%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `181760.958 ops/s` | `179648.956 ops/s` | `-1.16%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3620176.115 ops/s` | `3366995.872 ops/s` | `-6.99%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `2829045.454 ops/s` | `2874835.078 ops/s` | `+1.62%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1518465.384 ops/s` | `1656156.558 ops/s` | `+9.07%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `279.302 ms/op` | `279.822 ms/op` | `+0.19%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `303.647 ms/op` | `300.809 ms/op` | `-0.93%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `277.574 ms/op` | `275.725 ms/op` | `-0.67%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `540231.062 ops/s` | `505436.130 ops/s` | `-6.44%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `534866.355 ops/s` | `500053.730 ops/s` | `-6.51%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5364.707 ops/s` | `5382.399 ops/s` | `+0.33%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `267201.644 ops/s` | `266233.655 ops/s` | `-0.36%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `265743.145 ops/s` | `264766.011 ops/s` | `-0.37%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1458.499 ops/s` | `1467.644 ops/s` | `+0.63%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2581.718 ops/s` | `2492.965 ops/s` | `-3.44%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2760.706 ops/s` | `2720.308 ops/s` | `-1.46%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2477.344 ops/s` | `2415.769 ops/s` | `-2.49%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2760.191 ops/s` | `2697.550 ops/s` | `-2.27%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8250598.194 ops/s` | `8297125.269 ops/s` | `+0.56%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7722500.441 ops/s` | `7741783.359 ops/s` | `+0.25%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `7813714.748 ops/s` | `8562725.407 ops/s` | `+9.59%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `6759888.782 ops/s` | `6800997.440 ops/s` | `+0.61%` | `neutral` |
