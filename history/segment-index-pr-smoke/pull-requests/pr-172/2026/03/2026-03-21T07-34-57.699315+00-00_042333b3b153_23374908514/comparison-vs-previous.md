# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e6adc34214c9ff0a3d8eae146bfacb8b07282732`
- Candidate SHA: `042333b3b153fc6f9bdf02c43a8da74bc8b77d31`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `88.460 ops/s` | `87.218 ops/s` | `-1.40%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `87.764 ops/s` | `92.337 ops/s` | `+5.21%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `162456.851 ops/s` | `175644.598 ops/s` | `+8.12%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3802461.059 ops/s` | `3814384.313 ops/s` | `+0.31%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `158685.938 ops/s` | `162329.012 ops/s` | `+2.30%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4264371.226 ops/s` | `4166077.661 ops/s` | `-2.30%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `164794.115 ops/s` | `173012.243 ops/s` | `+4.99%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3908235.306 ops/s` | `4009388.670 ops/s` | `+2.59%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54248.211 ops/s` | `58803.521 ops/s` | `+8.40%` | `better` |
| `segment-index-get-persisted:getHitSync` | `102377.920 ops/s` | `108869.977 ops/s` | `+6.34%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `159267.800 ops/s` | `172233.034 ops/s` | `+8.14%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3885076.470 ops/s` | `3867876.676 ops/s` | `-0.44%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3155634.197 ops/s` | `3150878.309 ops/s` | `-0.15%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1669155.100 ops/s` | `1672877.794 ops/s` | `+0.22%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `436911.203 ops/s` | `440641.021 ops/s` | `+0.85%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `431746.206 ops/s` | `435413.162 ops/s` | `+0.85%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5164.997 ops/s` | `5227.858 ops/s` | `+1.22%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `204935.041 ops/s` | `185968.176 ops/s` | `-9.26%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `202333.829 ops/s` | `183410.063 ops/s` | `-9.35%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2601.212 ops/s` | `2558.113 ops/s` | `-1.66%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2128.390 ops/s` | `1442.195 ops/s` | `-32.24%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2324.270 ops/s` | `1998.449 ops/s` | `-14.02%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2044.173 ops/s` | `1557.817 ops/s` | `-23.79%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2235.482 ops/s` | `1660.377 ops/s` | `-25.73%` | `worse` |
