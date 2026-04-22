# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `f4ccddd061a9d27c819bed66a15005792d0cee05`
- Candidate SHA: `caf97a463d8e64a35b3a3de0c071788dd2255c74`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1874425.780 ops/s` | `1915312.607 ops/s` | `+2.18%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2412301.174 ops/s` | `2355559.415 ops/s` | `-2.35%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `13439.483 ops/s` | `13395.550 ops/s` | `-0.33%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `2378477.254 ops/s` | `2422037.804 ops/s` | `+1.83%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `102543.030 ops/s` | `107911.271 ops/s` | `+5.24%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2459117.278 ops/s` | `2402963.798 ops/s` | `-2.28%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1867780.519 ops/s` | `1873826.624 ops/s` | `+0.32%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1051179.394 ops/s` | `1062290.981 ops/s` | `+1.06%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `244985.063 ops/s` | `249429.913 ops/s` | `+1.81%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `121608.317 ops/s` | `130841.028 ops/s` | `+7.59%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `123376.746 ops/s` | `118588.885 ops/s` | `-3.88%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `56003.354 ops/s` | `55729.662 ops/s` | `-0.49%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `40890.060 ops/s` | `41517.807 ops/s` | `+1.54%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15113.294 ops/s` | `14211.855 ops/s` | `-5.96%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2513.900 ops/s` | `3288.388 ops/s` | `+30.81%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1864.546 ops/s` | `2483.591 ops/s` | `+33.20%` | `better` |
