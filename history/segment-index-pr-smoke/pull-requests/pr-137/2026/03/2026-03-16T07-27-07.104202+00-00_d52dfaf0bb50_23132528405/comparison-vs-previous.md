# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `d52dfaf0bb500d21eca06c7f958ce9ec35c3b8ab`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `104.624 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `96.228 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `171804.444 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `169059.114 ops/s` | `+5.56%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5266163.680 ops/s` | `+11.35%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `171331.310 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6741534.873 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `58751.501 ops/s` | `+7.69%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `107943.403 ops/s` | `+4.01%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `170732.682 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `7412008.163 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `488482.083 ops/s` | `+8.90%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `482637.777 ops/s` | `+9.00%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5844.306 ops/s` | `+0.95%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `212641.207 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `209952.908 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2688.300 ops/s` | `-` | `new` |
