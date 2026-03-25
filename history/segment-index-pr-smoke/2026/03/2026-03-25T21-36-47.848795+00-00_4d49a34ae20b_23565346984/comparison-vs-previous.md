# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4d49a34ae20bb8595d3ac3657abe6c2cde168e52`
- Candidate SHA: `4d49a34ae20bb8595d3ac3657abe6c2cde168e52`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.894 ops/s` | `87.894 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `83.475 ops/s` | `83.475 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `166671.207 ops/s` | `166671.207 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3818353.259 ops/s` | `3818353.259 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163079.763 ops/s` | `163079.763 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4181836.832 ops/s` | `4181836.832 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `169682.062 ops/s` | `169682.062 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `4051237.012 ops/s` | `4051237.012 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56573.614 ops/s` | `56573.614 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `99751.629 ops/s` | `99751.629 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164694.248 ops/s` | `164694.248 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4019268.170 ops/s` | `4019268.170 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2983298.781 ops/s` | `2983298.781 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1626220.014 ops/s` | `1626220.014 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `464940.491 ops/s` | `464940.491 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `459664.632 ops/s` | `459664.632 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5275.859 ops/s` | `5275.859 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200163.770 ops/s` | `200163.770 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `197616.070 ops/s` | `197616.070 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2547.700 ops/s` | `2547.700 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2184.247 ops/s` | `2184.247 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2482.403 ops/s` | `2482.403 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2235.485 ops/s` | `2235.485 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2463.563 ops/s` | `2463.563 ops/s` | `+0.00%` | `neutral` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7891969.477 ops/s` | `7891969.477 ops/s` | `+0.00%` | `neutral` |
| `sorted-data-diff-key-read:readNextKey` | `6608303.151 ops/s` | `6608303.151 ops/s` | `+0.00%` | `neutral` |
