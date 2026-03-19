# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6885ee00040024ba5bc2fc1f0365778414d7b12d`
- Candidate SHA: `f5f65f8ea0c4710a79321edc81f55876ad54a496`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.609 ops/s` | `84.952 ops/s` | `-3.03%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `88.363 ops/s` | `92.888 ops/s` | `+5.12%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `173686.983 ops/s` | `174509.712 ops/s` | `+0.47%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3630445.192 ops/s` | `3920397.878 ops/s` | `+7.99%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `171968.143 ops/s` | `171866.947 ops/s` | `-0.06%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4171023.951 ops/s` | `4268503.496 ops/s` | `+2.34%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `178958.864 ops/s` | `176614.636 ops/s` | `-1.31%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `4132026.927 ops/s` | `3827224.932 ops/s` | `-7.38%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56673.868 ops/s` | `57340.012 ops/s` | `+1.18%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `96831.319 ops/s` | `105011.439 ops/s` | `+8.45%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `170497.059 ops/s` | `176490.760 ops/s` | `+3.52%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4027334.805 ops/s` | `4188933.161 ops/s` | `+4.01%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2885020.806 ops/s` | `3052353.991 ops/s` | `+5.80%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1728089.397 ops/s` | `1638185.581 ops/s` | `-5.20%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `399976.465 ops/s` | `385000.847 ops/s` | `-3.74%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `394968.151 ops/s` | `379951.561 ops/s` | `-3.80%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5008.313 ops/s` | `5049.286 ops/s` | `+0.82%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `252045.515 ops/s` | `182559.950 ops/s` | `-27.57%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `185742.081 ops/s` | `179940.864 ops/s` | `-3.12%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `66303.434 ops/s` | `2619.086 ops/s` | `-96.05%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1561.125 ops/s` | `1472.319 ops/s` | `-5.69%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `1411.754 ops/s` | `1495.846 ops/s` | `+5.96%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1323.613 ops/s` | `1327.745 ops/s` | `+0.31%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `1350.162 ops/s` | `1708.602 ops/s` | `+26.55%` | `better` |
