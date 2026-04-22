# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `18a53ed9711d621c676b011285c6999f34435de1`
- Candidate SHA: `f4ccddd061a9d27c819bed66a15005792d0cee05`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1957733.620 ops/s` | `2263026.515 ops/s` | `+15.59%` | `better` |
| `segment-index-get-live:getMissSync` | `3734289.030 ops/s` | `3624393.542 ops/s` | `-2.94%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7405.149 ops/s` | `7131.560 ops/s` | `-3.69%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3598229.782 ops/s` | `3696448.057 ops/s` | `+2.73%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `110502.130 ops/s` | `115996.034 ops/s` | `+4.97%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3729254.140 ops/s` | `3775882.925 ops/s` | `+1.25%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2157338.649 ops/s` | `2191356.293 ops/s` | `+1.58%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1113236.440 ops/s` | `1108202.747 ops/s` | `-0.45%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `296230.519 ops/s` | `315703.592 ops/s` | `+6.57%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `143455.083 ops/s` | `211330.971 ops/s` | `+47.32%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `152775.435 ops/s` | `104372.622 ops/s` | `-31.68%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `72039.425 ops/s` | `33142.412 ops/s` | `-53.99%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `62336.666 ops/s` | `17215.200 ops/s` | `-72.38%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `9702.759 ops/s` | `15927.212 ops/s` | `+64.15%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2807.870 ops/s` | `2773.568 ops/s` | `-1.22%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2780.927 ops/s` | `2670.402 ops/s` | `-3.97%` | `warning` |
