# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot,segment-index-mixed-drain,segment-index-mixed-split-heavy`


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4d49a34ae20bb8595d3ac3657abe6c2cde168e52`
- Candidate SHA: `a87e6e844c0f83bfc93f8ff3748153d6ffec3dbb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.894 ops/s` | `84.792 ops/s` | `-3.53%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `83.475 ops/s` | `94.134 ops/s` | `+12.77%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `166671.207 ops/s` | `165180.641 ops/s` | `-0.89%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3818353.259 ops/s` | `3643691.940 ops/s` | `-4.57%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163079.763 ops/s` | `159118.489 ops/s` | `-2.43%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4181836.832 ops/s` | `4323633.189 ops/s` | `+3.39%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `169682.062 ops/s` | `164390.147 ops/s` | `-3.12%` | `warning` |
| `segment-index-get-overlay:getMissSync` | `4051237.012 ops/s` | `3876967.778 ops/s` | `-4.30%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56573.614 ops/s` | `54210.066 ops/s` | `-4.18%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `99751.629 ops/s` | `107048.121 ops/s` | `+7.31%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164694.248 ops/s` | `164494.979 ops/s` | `-0.12%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4019268.170 ops/s` | `3988713.083 ops/s` | `-0.76%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2983298.781 ops/s` | `3168517.519 ops/s` | `+6.21%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1626220.014 ops/s` | `1722313.062 ops/s` | `+5.91%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `464940.491 ops/s` | `434091.781 ops/s` | `-6.63%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `459664.632 ops/s` | `428941.347 ops/s` | `-6.68%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5275.859 ops/s` | `5150.434 ops/s` | `-2.38%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200163.770 ops/s` | `196205.539 ops/s` | `-1.98%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `197616.070 ops/s` | `193517.358 ops/s` | `-2.07%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2547.700 ops/s` | `2688.181 ops/s` | `+5.51%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2184.247 ops/s` | `2225.183 ops/s` | `+1.87%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2482.403 ops/s` | `2489.078 ops/s` | `+0.27%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2235.485 ops/s` | `2165.020 ops/s` | `-3.15%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2463.563 ops/s` | `2523.412 ops/s` | `+2.43%` | `neutral` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7891969.477 ops/s` | `7846877.342 ops/s` | `-0.57%` | `neutral` |
| `sorted-data-diff-key-read:readNextKey` | `6608303.151 ops/s` | `6241634.759 ops/s` | `-5.55%` | `warning` |
