# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `243589024837d3a342dfac1a029bea50b0292dac`
- Candidate SHA: `e6adc34214c9ff0a3d8eae146bfacb8b07282732`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `84.950 ops/s` | `88.460 ops/s` | `+4.13%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `81.933 ops/s` | `87.764 ops/s` | `+7.12%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `175110.870 ops/s` | `162456.851 ops/s` | `-7.23%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4004826.458 ops/s` | `3802461.059 ops/s` | `-5.05%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `173067.422 ops/s` | `158685.938 ops/s` | `-8.31%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `4330771.981 ops/s` | `4264371.226 ops/s` | `-1.53%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `169565.290 ops/s` | `164794.115 ops/s` | `-2.81%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3873632.888 ops/s` | `3908235.306 ops/s` | `+0.89%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `58807.747 ops/s` | `54248.211 ops/s` | `-7.75%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `101466.028 ops/s` | `102377.920 ops/s` | `+0.90%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `177069.506 ops/s` | `159267.800 ops/s` | `-10.05%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3964737.986 ops/s` | `3885076.470 ops/s` | `-2.01%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3219916.787 ops/s` | `3155634.197 ops/s` | `-2.00%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1660762.853 ops/s` | `1669155.100 ops/s` | `+0.51%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `447160.910 ops/s` | `436911.203 ops/s` | `-2.29%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `441839.678 ops/s` | `431746.206 ops/s` | `-2.28%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5321.232 ops/s` | `5164.997 ops/s` | `-2.94%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `204566.868 ops/s` | `204935.041 ops/s` | `+0.18%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `202055.412 ops/s` | `202333.829 ops/s` | `+0.14%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2511.456 ops/s` | `2601.212 ops/s` | `+3.57%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1716.989 ops/s` | `2128.390 ops/s` | `+23.96%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1867.967 ops/s` | `2324.270 ops/s` | `+24.43%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1702.866 ops/s` | `2044.173 ops/s` | `+20.04%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1892.800 ops/s` | `2235.482 ops/s` | `+18.10%` | `better` |
