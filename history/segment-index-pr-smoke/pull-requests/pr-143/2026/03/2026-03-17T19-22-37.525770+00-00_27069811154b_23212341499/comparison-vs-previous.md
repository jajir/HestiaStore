# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `27069811154b5e93a9d6af9c20d3304c4319405a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `94.832 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `92.904 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `176136.528 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `7008808.667 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `174924.313 ops/s` | `+9.22%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5242631.763 ops/s` | `+10.85%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `173773.387 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6742584.410 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57283.890 ops/s` | `+5.00%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `110346.568 ops/s` | `+6.32%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `175796.844 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6800059.635 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `454403.422 ops/s` | `+1.30%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `448780.292 ops/s` | `+1.35%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5623.129 ops/s` | `-2.87%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `199840.270 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `196885.674 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2954.596 ops/s` | `-` | `new` |
