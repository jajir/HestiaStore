# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `f110d0a08518e6d6f918649d4aa14d48cfb15719`
- Candidate SHA: `e283e03acfa6fee69587913c5037298a9d2795d4`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `92.724 ops/s` | `83.940 ops/s` | `-9.47%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `94.344 ops/s` | `87.915 ops/s` | `-6.81%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `167070.807 ops/s` | `165434.985 ops/s` | `-0.98%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3699544.310 ops/s` | `3917421.311 ops/s` | `+5.89%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163403.301 ops/s` | `163309.687 ops/s` | `-0.06%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4129908.406 ops/s` | `4230312.818 ops/s` | `+2.43%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `164387.207 ops/s` | `166014.382 ops/s` | `+0.99%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `4179974.819 ops/s` | `3893940.738 ops/s` | `-6.84%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54452.371 ops/s` | `54034.426 ops/s` | `-0.77%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `112946.465 ops/s` | `92363.306 ops/s` | `-18.22%` | `worse` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165521.969 ops/s` | `167813.185 ops/s` | `+1.38%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4287807.936 ops/s` | `3808467.325 ops/s` | `-11.18%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3154576.602 ops/s` | `3113806.130 ops/s` | `-1.29%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1663719.970 ops/s` | `1730631.857 ops/s` | `+4.02%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `446507.488 ops/s` | `446774.077 ops/s` | `+0.06%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `441223.648 ops/s` | `441623.712 ops/s` | `+0.09%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5283.840 ops/s` | `5150.364 ops/s` | `-2.53%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `189316.043 ops/s` | `195588.077 ops/s` | `+3.31%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `186881.518 ops/s` | `193028.078 ops/s` | `+3.29%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2434.525 ops/s` | `2559.999 ops/s` | `+5.15%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2286.619 ops/s` | `2318.757 ops/s` | `+1.41%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2515.121 ops/s` | `2574.753 ops/s` | `+2.37%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2234.274 ops/s` | `2227.181 ops/s` | `-0.32%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2413.370 ops/s` | `2498.924 ops/s` | `+3.55%` | `better` |
