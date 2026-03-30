# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `49.149 ops/s` | `49.210 ops/s` | `+0.12%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `41.284 ops/s` | `53.451 ops/s` | `+29.47%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `170691.355 ops/s` | `189652.203 ops/s` | `+11.11%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3789519.722 ops/s` | `3383853.921 ops/s` | `-10.70%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `99.306 ops/s` | `102.706 ops/s` | `+3.42%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `87.089 ops/s` | `93.622 ops/s` | `+7.50%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `172919.374 ops/s` | `187115.271 ops/s` | `+8.21%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3689828.168 ops/s` | `3784684.033 ops/s` | `+2.57%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `164465.899 ops/s` | `173290.313 ops/s` | `+5.37%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4031392.852 ops/s` | `4056861.740 ops/s` | `+0.63%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `164496.608 ops/s` | `171412.321 ops/s` | `+4.20%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3831181.641 ops/s` | `3627570.883 ops/s` | `-5.31%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `65222.553 ops/s` | `65163.374 ops/s` | `-0.09%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `116905.829 ops/s` | `117874.457 ops/s` | `+0.83%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164316.639 ops/s` | `170421.306 ops/s` | `+3.72%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3817072.354 ops/s` | `3849279.951 ops/s` | `+0.84%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2752777.682 ops/s` | `3105910.846 ops/s` | `+12.83%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1805415.060 ops/s` | `1615766.699 ops/s` | `-10.50%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.454 ms/op` | `244.838 ms/op` | `+0.16%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `266.076 ms/op` | `266.096 ms/op` | `+0.01%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `242.811 ms/op` | `244.746 ms/op` | `+0.80%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `514554.424 ops/s` | `569980.643 ops/s` | `+10.77%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `509243.784 ops/s` | `564631.826 ops/s` | `+10.88%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5310.640 ops/s` | `5348.817 ops/s` | `+0.72%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `252526.048 ops/s` | `275434.627 ops/s` | `+9.07%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `250788.639 ops/s` | `274012.423 ops/s` | `+9.26%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1737.409 ops/s` | `1422.204 ops/s` | `-18.14%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2011.060 ops/s` | `1611.435 ops/s` | `-19.87%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2198.882 ops/s` | `1641.004 ops/s` | `-25.37%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1994.897 ops/s` | `1531.500 ops/s` | `-23.23%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2187.751 ops/s` | `1600.170 ops/s` | `-26.86%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8466009.739 ops/s` | `8536502.081 ops/s` | `+0.83%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7717121.000 ops/s` | `7931647.282 ops/s` | `+2.78%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8864130.379 ops/s` | `9481033.142 ops/s` | `+6.96%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `6950712.499 ops/s` | `7504162.838 ops/s` | `+7.96%` | `better` |
