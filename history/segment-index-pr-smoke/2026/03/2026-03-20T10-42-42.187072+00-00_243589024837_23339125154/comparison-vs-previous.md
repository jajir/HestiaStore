# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ec14f87a3a2d8100d259761a91d659b7d5a9a8cc`
- Candidate SHA: `243589024837d3a342dfac1a029bea50b0292dac`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `90.108 ops/s` | `84.950 ops/s` | `-5.72%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `88.516 ops/s` | `81.933 ops/s` | `-7.44%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `176173.841 ops/s` | `175110.870 ops/s` | `-0.60%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3870286.911 ops/s` | `4004826.458 ops/s` | `+3.48%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `166573.440 ops/s` | `173067.422 ops/s` | `+3.90%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4415716.444 ops/s` | `4330771.981 ops/s` | `-1.92%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `172616.463 ops/s` | `169565.290 ops/s` | `-1.77%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3802980.283 ops/s` | `3873632.888 ops/s` | `+1.86%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `57704.870 ops/s` | `58807.747 ops/s` | `+1.91%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `107617.573 ops/s` | `101466.028 ops/s` | `-5.72%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `176327.908 ops/s` | `177069.506 ops/s` | `+0.42%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4021547.548 ops/s` | `3964737.986 ops/s` | `-1.41%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3090090.064 ops/s` | `3219916.787 ops/s` | `+4.20%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1708873.695 ops/s` | `1660762.853 ops/s` | `-2.82%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `439212.694 ops/s` | `447160.910 ops/s` | `+1.81%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `434020.468 ops/s` | `441839.678 ops/s` | `+1.80%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5192.226 ops/s` | `5321.232 ops/s` | `+2.48%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `199908.647 ops/s` | `204566.868 ops/s` | `+2.33%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `197431.760 ops/s` | `202055.412 ops/s` | `+2.34%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2476.887 ops/s` | `2511.456 ops/s` | `+1.40%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1697.035 ops/s` | `1716.989 ops/s` | `+1.18%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `1788.772 ops/s` | `1867.967 ops/s` | `+4.43%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1613.467 ops/s` | `1702.866 ops/s` | `+5.54%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2097.777 ops/s` | `1892.800 ops/s` | `-9.77%` | `worse` |
