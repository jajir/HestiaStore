# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `b748db3ff4fe240d4ffc1b5ce9ade20b811c22ca`
- Candidate SHA: `2c5fe7ae1a709d756b1b7bea541b3b48cc517057`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `99.064 ops/s` | `90.970 ops/s` | `-8.17%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `89.365 ops/s` | `87.718 ops/s` | `-1.84%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `177384.393 ops/s` | `176136.840 ops/s` | `-0.70%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `6676312.422 ops/s` | `3954104.801 ops/s` | `-40.77%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `168255.736 ops/s` | `166943.326 ops/s` | `-0.78%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `5166690.492 ops/s` | `4297167.449 ops/s` | `-16.83%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `166630.342 ops/s` | `174498.022 ops/s` | `+4.72%` | `better` |
| `segment-index-get-overlay:getMissSync` | `7410294.894 ops/s` | `4153477.350 ops/s` | `-43.95%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55180.206 ops/s` | `58599.630 ops/s` | `+6.20%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103580.631 ops/s` | `106224.718 ops/s` | `+2.55%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `178536.235 ops/s` | `167371.445 ops/s` | `-6.25%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `7040012.841 ops/s` | `3883748.021 ops/s` | `-44.83%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `-` | `3150706.758 ops/s` | `-` | `new` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `-` | `1669741.383 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `456260.045 ops/s` | `465736.563 ops/s` | `+2.08%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450400.156 ops/s` | `460485.115 ops/s` | `+2.24%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5859.889 ops/s` | `5251.448 ops/s` | `-10.38%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `197348.590 ops/s` | `196648.917 ops/s` | `-0.35%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `194523.892 ops/s` | `193935.102 ops/s` | `-0.30%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2824.699 ops/s` | `2713.816 ops/s` | `-3.93%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `-` | `2015.588 ops/s` | `-` | `new` |
| `segment-index-persisted-mutation:deleteSync` | `-` | `2407.824 ops/s` | `-` | `new` |
| `segment-index-persisted-mutation:putAsyncJoin` | `-` | `2054.697 ops/s` | `-` | `new` |
| `segment-index-persisted-mutation:putSync` | `-` | `2263.341 ops/s` | `-` | `new` |
