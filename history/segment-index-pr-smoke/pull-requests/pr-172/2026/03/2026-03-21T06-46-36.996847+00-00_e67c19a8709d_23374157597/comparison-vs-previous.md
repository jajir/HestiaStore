# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e6adc34214c9ff0a3d8eae146bfacb8b07282732`
- Candidate SHA: `e67c19a8709d85f0ccd3ef37a8947050a122d94a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `88.460 ops/s` | `87.139 ops/s` | `-1.49%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `87.764 ops/s` | `89.245 ops/s` | `+1.69%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `162456.851 ops/s` | `167579.292 ops/s` | `+3.15%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3802461.059 ops/s` | `3792966.669 ops/s` | `-0.25%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `158685.938 ops/s` | `159047.613 ops/s` | `+0.23%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4264371.226 ops/s` | `4055119.725 ops/s` | `-4.91%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `164794.115 ops/s` | `167467.447 ops/s` | `+1.62%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3908235.306 ops/s` | `3764801.097 ops/s` | `-3.67%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54248.211 ops/s` | `56270.819 ops/s` | `+3.73%` | `better` |
| `segment-index-get-persisted:getHitSync` | `102377.920 ops/s` | `111826.158 ops/s` | `+9.23%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `159267.800 ops/s` | `157522.765 ops/s` | `-1.10%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3885076.470 ops/s` | `4044173.401 ops/s` | `+4.10%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3155634.197 ops/s` | `3017263.183 ops/s` | `-4.38%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1669155.100 ops/s` | `1632124.433 ops/s` | `-2.22%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `436911.203 ops/s` | `429320.045 ops/s` | `-1.74%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `431746.206 ops/s` | `424156.541 ops/s` | `-1.76%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5164.997 ops/s` | `5163.504 ops/s` | `-0.03%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `204935.041 ops/s` | `197638.430 ops/s` | `-3.56%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `202333.829 ops/s` | `195083.944 ops/s` | `-3.58%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2601.212 ops/s` | `2554.486 ops/s` | `-1.80%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2128.390 ops/s` | `2222.075 ops/s` | `+4.40%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2324.270 ops/s` | `2555.070 ops/s` | `+9.93%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2044.173 ops/s` | `2211.510 ops/s` | `+8.19%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2235.482 ops/s` | `2462.326 ops/s` | `+10.15%` | `better` |
