# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `314ee53176fcebd2eec3e70c90db11224f3812ea`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `102.583 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `94.231 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `175063.944 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6247325.895 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `171398.477 ops/s` | `+7.02%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5290418.253 ops/s` | `+11.86%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `170200.315 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `7166757.926 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `59892.007 ops/s` | `+9.78%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `99425.923 ops/s` | `-4.20%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `176105.986 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6865712.177 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `457976.018 ops/s` | `+2.10%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `452039.832 ops/s` | `+2.09%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5936.186 ops/s` | `+2.54%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `200761.833 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `197769.226 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2992.607 ops/s` | `-` | `new` |
