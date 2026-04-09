# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `5d7bf982f97f83973c55d33d3cc3fdb0febef0b1`
- Candidate SHA: `f72ba80fa547fdd7f1b82bb5bc113069648a84fa`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitAsyncJoin` | `165808.544 ops/s` | `154609.702 ops/s` | `-6.75%` | `warning` |
| `segment-index-get-live:getHitSync` | `3237069.623 ops/s` | `3738446.868 ops/s` | `+15.49%` | `better` |
| `segment-index-get-live:getMissAsyncJoin` | `163438.849 ops/s` | `155923.065 ops/s` | `-4.60%` | `warning` |
| `segment-index-get-live:getMissSync` | `4054287.235 ops/s` | `3978315.661 ops/s` | `-1.87%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.401 ops/s` | `165.849 ops/s` | `+89.76%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `95.539 ops/s` | `61.991 ops/s` | `-35.11%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `167405.367 ops/s` | `172602.567 ops/s` | `+3.10%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2632691.123 ops/s` | `4019789.531 ops/s` | `+52.69%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55248.320 ops/s` | `61446.374 ops/s` | `+11.22%` | `better` |
| `segment-index-get-persisted:getHitSync` | `115422.804 ops/s` | `120158.652 ops/s` | `+4.10%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164384.971 ops/s` | `163539.789 ops/s` | `-0.51%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4057504.229 ops/s` | `4085605.487 ops/s` | `+0.69%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2486597.268 ops/s` | `2473592.714 ops/s` | `-0.52%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1536799.173 ops/s` | `1510591.363 ops/s` | `-1.71%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `372915.891 ops/s` | `373476.707 ops/s` | `+0.15%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `202735.593 ops/s` | `194443.574 ops/s` | `-4.09%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170180.298 ops/s` | `179033.133 ops/s` | `+5.20%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `51417.070 ops/s` | `27569.266 ops/s` | `-46.38%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `44746.916 ops/s` | `18442.084 ops/s` | `-58.79%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `6670.154 ops/s` | `9127.183 ops/s` | `+36.84%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2246.783 ops/s` | `2188.984 ops/s` | `-2.57%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2581.084 ops/s` | `2207.567 ops/s` | `-14.47%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2256.645 ops/s` | `2227.858 ops/s` | `-1.28%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2568.107 ops/s` | `2430.175 ops/s` | `-5.37%` | `warning` |
