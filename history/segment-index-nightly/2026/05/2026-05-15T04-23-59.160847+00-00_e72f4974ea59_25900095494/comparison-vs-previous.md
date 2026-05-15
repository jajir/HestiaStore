# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-lifecycle`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.863 ops/s` | `46.454 ops/s` | `+10.97%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `42.981 ops/s` | `54.832 ops/s` | `+27.57%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `184735.600 ops/s` | `169495.920 ops/s` | `-8.25%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `3610324.610 ops/s` | `3798228.274 ops/s` | `+5.20%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `104.175 ops/s` | `110.099 ops/s` | `+5.69%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `83.324 ops/s` | `77.654 ops/s` | `-6.81%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `183575.570 ops/s` | `167937.645 ops/s` | `-8.52%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3614657.196 ops/s` | `4031623.979 ops/s` | `+11.54%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `178749.820 ops/s` | `154462.019 ops/s` | `-13.59%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `3646159.138 ops/s` | `3766668.554 ops/s` | `+3.31%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `184503.765 ops/s` | `154795.300 ops/s` | `-16.10%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `3607900.995 ops/s` | `3646821.216 ops/s` | `+1.08%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63460.522 ops/s` | `60426.549 ops/s` | `-4.78%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `115531.183 ops/s` | `115830.255 ops/s` | `+0.26%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `184227.258 ops/s` | `163547.865 ops/s` | `-11.22%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3492257.946 ops/s` | `3784183.350 ops/s` | `+8.36%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2514225.996 ops/s` | `2949210.104 ops/s` | `+17.30%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1541006.836 ops/s` | `1664425.941 ops/s` | `+8.01%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `277.935 ms/op` | `247.971 ms/op` | `-10.78%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `303.057 ms/op` | `270.451 ms/op` | `-10.76%` | `worse` |
| `segment-index-lifecycle:openExisting` | `274.227 ms/op` | `245.891 ms/op` | `-10.33%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `506383.677 ops/s` | `512603.681 ops/s` | `+1.23%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `501022.709 ops/s` | `507291.146 ops/s` | `+1.25%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5360.969 ops/s` | `5312.535 ops/s` | `-0.90%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `258172.425 ops/s` | `266903.077 ops/s` | `+3.38%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `256720.898 ops/s` | `265421.482 ops/s` | `+3.39%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1451.527 ops/s` | `1481.595 ops/s` | `+2.07%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2468.930 ops/s` | `1932.619 ops/s` | `-21.72%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2689.194 ops/s` | `2240.396 ops/s` | `-16.69%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2410.955 ops/s` | `1944.254 ops/s` | `-19.36%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2633.067 ops/s` | `2176.611 ops/s` | `-17.34%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8304714.073 ops/s` | `8431583.726 ops/s` | `+1.53%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7527495.602 ops/s` | `7995691.979 ops/s` | `+6.22%` | `better` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8477335.790 ops/s` | `9435275.787 ops/s` | `+11.30%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `6672990.998 ops/s` | `7473315.219 ops/s` | `+11.99%` | `better` |
