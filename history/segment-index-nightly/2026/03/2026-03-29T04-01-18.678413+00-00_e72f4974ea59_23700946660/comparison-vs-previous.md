# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `40.442 ops/s` | `49.149 ops/s` | `+21.53%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `39.899 ops/s` | `41.284 ops/s` | `+3.47%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `172668.754 ops/s` | `170691.355 ops/s` | `-1.15%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3529537.796 ops/s` | `3789519.722 ops/s` | `+7.37%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `86.016 ops/s` | `99.306 ops/s` | `+15.45%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `86.939 ops/s` | `87.089 ops/s` | `+0.17%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `173572.438 ops/s` | `172919.374 ops/s` | `-0.38%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3705152.794 ops/s` | `3689828.168 ops/s` | `-0.41%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `165068.243 ops/s` | `164465.899 ops/s` | `-0.36%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4135409.171 ops/s` | `4031392.852 ops/s` | `-2.52%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `165576.345 ops/s` | `164496.608 ops/s` | `-0.65%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3654703.794 ops/s` | `3831181.641 ops/s` | `+4.83%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `65809.027 ops/s` | `65222.553 ops/s` | `-0.89%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `117132.575 ops/s` | `116905.829 ops/s` | `-0.19%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `159974.707 ops/s` | `164316.639 ops/s` | `+2.71%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4043931.381 ops/s` | `3817072.354 ops/s` | `-5.61%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `3069010.783 ops/s` | `2752777.682 ops/s` | `-10.30%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1538674.372 ops/s` | `1805415.060 ops/s` | `+17.34%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `246.028 ms/op` | `244.454 ms/op` | `-0.64%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `265.962 ms/op` | `266.076 ms/op` | `+0.04%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `241.212 ms/op` | `242.811 ms/op` | `+0.66%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `520241.916 ops/s` | `514554.424 ops/s` | `-1.09%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `514938.477 ops/s` | `509243.784 ops/s` | `-1.11%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5303.439 ops/s` | `5310.640 ops/s` | `+0.14%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `264443.893 ops/s` | `252526.048 ops/s` | `-4.51%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `262976.243 ops/s` | `250788.639 ops/s` | `-4.63%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1467.650 ops/s` | `1737.409 ops/s` | `+18.38%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2012.821 ops/s` | `2011.060 ops/s` | `-0.09%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2135.670 ops/s` | `2198.882 ops/s` | `+2.96%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1976.802 ops/s` | `1994.897 ops/s` | `+0.92%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2101.405 ops/s` | `2187.751 ops/s` | `+4.11%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8424501.287 ops/s` | `8466009.739 ops/s` | `+0.49%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7914288.112 ops/s` | `7717121.000 ops/s` | `-2.49%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8532913.454 ops/s` | `8864130.379 ops/s` | `+3.88%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `7339270.281 ops/s` | `6950712.499 ops/s` | `-5.29%` | `warning` |
