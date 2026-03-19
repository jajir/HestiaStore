# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67ecf099e47860f6b644e6e59ef58bc83f0c7dda`
- Candidate SHA: `e969ecef1e771a96d562bc407b89812ad3256a81`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `99.078 ops/s` | `86.624 ops/s` | `-12.57%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `94.820 ops/s` | `91.627 ops/s` | `-3.37%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `175405.999 ops/s` | `175523.036 ops/s` | `+0.07%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `6729617.197 ops/s` | `4042889.262 ops/s` | `-39.92%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `173782.071 ops/s` | `174660.921 ops/s` | `+0.51%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `5401085.738 ops/s` | `4333311.899 ops/s` | `-19.77%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `176099.442 ops/s` | `177116.644 ops/s` | `+0.58%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `6920008.833 ops/s` | `4146800.619 ops/s` | `-40.08%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `57870.377 ops/s` | `60496.076 ops/s` | `+4.54%` | `better` |
| `segment-index-get-persisted:getHitSync` | `112270.450 ops/s` | `112310.911 ops/s` | `+0.04%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `176113.712 ops/s` | `177061.385 ops/s` | `+0.54%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `7021215.081 ops/s` | `4151897.277 ops/s` | `-40.87%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `-` | `3151908.297 ops/s` | `-` | `new` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `-` | `1698649.237 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `497998.524 ops/s` | `477185.531 ops/s` | `-4.18%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `492177.624 ops/s` | `471926.031 ops/s` | `-4.11%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5820.900 ops/s` | `5259.500 ops/s` | `-9.64%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `209420.006 ops/s` | `194305.366 ops/s` | `-7.22%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `188312.800 ops/s` | `191878.190 ops/s` | `+1.89%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `21107.206 ops/s` | `2427.176 ops/s` | `-88.50%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `-` | `2017.038 ops/s` | `-` | `new` |
| `segment-index-persisted-mutation:deleteSync` | `-` | `2262.862 ops/s` | `-` | `new` |
| `segment-index-persisted-mutation:putAsyncJoin` | `-` | `1972.917 ops/s` | `-` | `new` |
| `segment-index-persisted-mutation:putSync` | `-` | `2160.653 ops/s` | `-` | `new` |
