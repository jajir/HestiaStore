# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `44.049 ops/s` | `48.884 ops/s` | `+10.98%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `37.493 ops/s` | `35.803 ops/s` | `-4.51%` | `warning` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `235880.691 ops/s` | `171641.098 ops/s` | `-27.23%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `4660940.362 ops/s` | `3751084.751 ops/s` | `-19.52%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `89.495 ops/s` | `94.212 ops/s` | `+5.27%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `94.822 ops/s` | `92.337 ops/s` | `-2.62%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `238474.304 ops/s` | `173048.267 ops/s` | `-27.44%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4740936.568 ops/s` | `4039407.259 ops/s` | `-14.80%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `235316.352 ops/s` | `159257.799 ops/s` | `-32.32%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `4542117.578 ops/s` | `4067505.767 ops/s` | `-10.45%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `238023.127 ops/s` | `166623.788 ops/s` | `-30.00%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `4087352.383 ops/s` | `4231170.250 ops/s` | `+3.52%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `71989.881 ops/s` | `63679.739 ops/s` | `-11.54%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `158932.503 ops/s` | `114880.721 ops/s` | `-27.72%` | `worse` |
| `segment-index-get-persisted:getMissAsyncJoin` | `234161.295 ops/s` | `163707.160 ops/s` | `-30.09%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4418976.383 ops/s` | `3548040.019 ops/s` | `-19.71%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3635838.264 ops/s` | `3167180.957 ops/s` | `-12.89%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1970228.788 ops/s` | `1532464.878 ops/s` | `-22.22%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `218.384 ms/op` | `243.768 ms/op` | `+11.62%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `232.611 ms/op` | `265.710 ms/op` | `+14.23%` | `better` |
| `segment-index-lifecycle:openExisting` | `215.507 ms/op` | `238.052 ms/op` | `+10.46%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `776792.675 ops/s` | `523263.851 ops/s` | `-32.64%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `771420.822 ops/s` | `517922.526 ops/s` | `-32.86%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5371.853 ops/s` | `5341.324 ops/s` | `-0.57%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `381139.999 ops/s` | `264643.785 ops/s` | `-30.57%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `379603.898 ops/s` | `263098.124 ops/s` | `-30.69%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1536.102 ops/s` | `1545.661 ops/s` | `+0.62%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `397.539 ops/s` | `2046.638 ops/s` | `+414.83%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `490.260 ops/s` | `2256.077 ops/s` | `+360.18%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `775.997 ops/s` | `2028.445 ops/s` | `+161.40%` | `better` |
| `segment-index-persisted-mutation:putSync` | `529.760 ops/s` | `2108.759 ops/s` | `+298.06%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `11059637.577 ops/s` | `8565823.167 ops/s` | `-22.55%` | `worse` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `10192809.952 ops/s` | `7665129.599 ops/s` | `-24.80%` | `worse` |
| `sorted-data-diff-key-read-compact:readNextKey` | `10819823.112 ops/s` | `9017111.023 ops/s` | `-16.66%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `8570061.666 ops/s` | `7536770.502 ops/s` | `-12.06%` | `worse` |
