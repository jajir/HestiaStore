# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-mixed-drain`


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4d49a34ae20bb8595d3ac3657abe6c2cde168e52`
- Candidate SHA: `5a9a4281b25f5991d0fc860dda1496eff103f304`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.894 ops/s` | `106.739 ops/s` | `+21.44%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `83.475 ops/s` | `95.126 ops/s` | `+13.96%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `166671.207 ops/s` | `174700.072 ops/s` | `+4.82%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3818353.259 ops/s` | `3792373.821 ops/s` | `-0.68%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163079.763 ops/s` | `172507.038 ops/s` | `+5.78%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4181836.832 ops/s` | `4343412.069 ops/s` | `+3.86%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `169682.062 ops/s` | `165183.182 ops/s` | `-2.65%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `4051237.012 ops/s` | `3890204.832 ops/s` | `-3.97%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56573.614 ops/s` | `55053.172 ops/s` | `-2.69%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `99751.629 ops/s` | `96493.999 ops/s` | `-3.27%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164694.248 ops/s` | `166794.019 ops/s` | `+1.27%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4019268.170 ops/s` | `4010761.198 ops/s` | `-0.21%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2983298.781 ops/s` | `3022740.785 ops/s` | `+1.32%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1626220.014 ops/s` | `1567994.824 ops/s` | `-3.58%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `464940.491 ops/s` | `395280.164 ops/s` | `-14.98%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `459664.632 ops/s` | `390135.631 ops/s` | `-15.13%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5275.859 ops/s` | `5131.696 ops/s` | `-2.73%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200163.770 ops/s` | `194290.611 ops/s` | `-2.93%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `197616.070 ops/s` | `191557.038 ops/s` | `-3.07%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2547.700 ops/s` | `2733.573 ops/s` | `+7.30%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2184.247 ops/s` | `2070.901 ops/s` | `-5.19%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2482.403 ops/s` | `2411.226 ops/s` | `-2.87%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2235.485 ops/s` | `2095.783 ops/s` | `-6.25%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2463.563 ops/s` | `2306.485 ops/s` | `-6.38%` | `warning` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7891969.477 ops/s` | `7767638.790 ops/s` | `-1.58%` | `neutral` |
| `sorted-data-diff-key-read:readNextKey` | `6608303.151 ops/s` | `6475242.595 ops/s` | `-2.01%` | `neutral` |
