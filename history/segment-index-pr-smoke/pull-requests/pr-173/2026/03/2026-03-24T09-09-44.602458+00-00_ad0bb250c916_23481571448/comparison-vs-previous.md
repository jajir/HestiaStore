# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ad0bb250c916e65c29bdeedc80e25018224ac737`
- Candidate SHA: `ad0bb250c916e65c29bdeedc80e25018224ac737`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `91.929 ops/s` | `91.929 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `94.054 ops/s` | `94.054 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `164137.858 ops/s` | `164137.858 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4029070.807 ops/s` | `4029070.807 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `161233.315 ops/s` | `161233.315 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4164549.705 ops/s` | `4164549.705 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `167211.059 ops/s` | `167211.059 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3922022.650 ops/s` | `3922022.650 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56314.208 ops/s` | `56314.208 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103737.417 ops/s` | `103737.417 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163299.426 ops/s` | `163299.426 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3932583.228 ops/s` | `3932583.228 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3123341.536 ops/s` | `3123341.536 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1685472.875 ops/s` | `1685472.875 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `425352.742 ops/s` | `425352.742 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `420219.316 ops/s` | `420219.316 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5133.426 ops/s` | `5133.426 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `208499.635 ops/s` | `208499.635 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `206068.020 ops/s` | `206068.020 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2431.615 ops/s` | `2431.615 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2217.538 ops/s` | `2217.538 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2510.353 ops/s` | `2510.353 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2214.729 ops/s` | `2214.729 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2471.631 ops/s` | `2471.631 ops/s` | `+0.00%` | `neutral` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7829425.123 ops/s` | `7829425.123 ops/s` | `+0.00%` | `neutral` |
| `sorted-data-diff-key-read:readNextKey` | `6734588.909 ops/s` | `6734588.909 ops/s` | `+0.00%` | `neutral` |
