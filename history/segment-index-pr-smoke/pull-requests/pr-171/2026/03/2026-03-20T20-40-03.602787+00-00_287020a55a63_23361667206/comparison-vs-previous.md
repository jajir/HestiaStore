# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `243589024837d3a342dfac1a029bea50b0292dac`
- Candidate SHA: `287020a55a63a9ac8c9dd7362ff7182fa7e6f2de`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `84.950 ops/s` | `94.735 ops/s` | `+11.52%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `81.933 ops/s` | `88.898 ops/s` | `+8.50%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `175110.870 ops/s` | `174745.034 ops/s` | `-0.21%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4004826.458 ops/s` | `3560433.213 ops/s` | `-11.10%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `173067.422 ops/s` | `173046.536 ops/s` | `-0.01%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4330771.981 ops/s` | `4202198.720 ops/s` | `-2.97%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `169565.290 ops/s` | `168327.602 ops/s` | `-0.73%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3873632.888 ops/s` | `4141309.407 ops/s` | `+6.91%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `58807.747 ops/s` | `57231.043 ops/s` | `-2.68%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `101466.028 ops/s` | `104553.955 ops/s` | `+3.04%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `177069.506 ops/s` | `173323.826 ops/s` | `-2.12%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3964737.986 ops/s` | `4045601.623 ops/s` | `+2.04%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3219916.787 ops/s` | `2942286.919 ops/s` | `-8.62%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1660762.853 ops/s` | `1617894.990 ops/s` | `-2.58%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `447160.910 ops/s` | `475919.409 ops/s` | `+6.43%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `441839.678 ops/s` | `470608.656 ops/s` | `+6.51%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5321.232 ops/s` | `5310.753 ops/s` | `-0.20%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `204566.868 ops/s` | `197032.119 ops/s` | `-3.68%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `202055.412 ops/s` | `194567.996 ops/s` | `-3.71%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2511.456 ops/s` | `2464.123 ops/s` | `-1.88%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1716.989 ops/s` | `1432.538 ops/s` | `-16.57%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `1867.967 ops/s` | `1899.011 ops/s` | `+1.66%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1702.866 ops/s` | `1670.430 ops/s` | `-1.90%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `1892.800 ops/s` | `1552.976 ops/s` | `-17.95%` | `worse` |
