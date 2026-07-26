# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `a63e8857313f97e6163f9e4567b7002fa0a469ea`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2156890.005 ops/s` | `2010223.279 ops/s` | `-6.80%` | `warning` |
| `segment-index-get-live:getMissSync` | `1848530.584 ops/s` | `1823057.094 ops/s` | `-1.38%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1511996.650 ops/s` | `1709100.470 ops/s` | `+13.04%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1882818.088 ops/s` | `1978065.950 ops/s` | `+5.06%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2131010.135 ops/s` | `2129029.586 ops/s` | `-0.09%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1121215.583 ops/s` | `1237417.676 ops/s` | `+10.36%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `256.580 ms/op` | `256.786 ms/op` | `+0.08%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `277.057 ms/op` | `275.188 ms/op` | `-0.67%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `251.018 ms/op` | `251.549 ms/op` | `+0.21%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `432043.348 ops/s` | `425939.591 ops/s` | `-1.41%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `199610.779 ops/s` | `191597.209 ops/s` | `-4.01%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `232432.569 ops/s` | `234342.383 ops/s` | `+0.82%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `893287.001 ops/s` | `849680.648 ops/s` | `-4.88%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `875745.685 ops/s` | `832702.706 ops/s` | `-4.92%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17541.316 ops/s` | `16977.942 ops/s` | `-3.21%` | `warning` |
