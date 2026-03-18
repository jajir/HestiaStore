# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `c118a313b1c3233b5ebe313b01b13aafe8aebd70`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `98.739 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `102.131 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `165267.095 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6365099.927 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `157159.604 ops/s` | `-1.87%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5250356.937 ops/s` | `+11.01%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `159421.694 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6600347.517 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `56063.299 ops/s` | `+2.76%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `103573.254 ops/s` | `-0.20%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `159189.069 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6776435.886 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `431401.579 ops/s` | `-3.83%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `425662.115 ops/s` | `-3.87%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5739.464 ops/s` | `-0.86%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `207656.603 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `204942.082 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2714.520 ops/s` | `-` | `new` |
