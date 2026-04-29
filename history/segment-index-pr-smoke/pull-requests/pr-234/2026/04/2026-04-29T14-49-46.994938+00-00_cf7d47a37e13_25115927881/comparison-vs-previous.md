# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67d6f015595d472d71dbaf369987d5f6eb97b5b5`
- Candidate SHA: `cf7d47a37e13ca77585977e33ec6dfad301ee502`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2434625.181 ops/s` | `2228987.447 ops/s` | `-8.45%` | `worse` |
| `segment-index-get-live:getMissSync` | `4235514.095 ops/s` | `3798067.841 ops/s` | `-10.33%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `7489.633 ops/s` | `7112.043 ops/s` | `-5.04%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3814528.118 ops/s` | `4064199.577 ops/s` | `+6.55%` | `better` |
| `segment-index-get-persisted:getHitSync` | `119572.179 ops/s` | `112614.908 ops/s` | `-5.82%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3751402.345 ops/s` | `3930153.602 ops/s` | `+4.76%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2010777.929 ops/s` | `2194758.405 ops/s` | `+9.15%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1033955.055 ops/s` | `1044007.983 ops/s` | `+0.97%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `283007.487 ops/s` | `315512.120 ops/s` | `+11.49%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `137276.471 ops/s` | `147366.826 ops/s` | `+7.35%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `145731.016 ops/s` | `168145.294 ops/s` | `+15.38%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `41280.902 ops/s` | `44328.587 ops/s` | `+7.38%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36114.855 ops/s` | `39015.160 ops/s` | `+8.03%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5166.048 ops/s` | `5313.427 ops/s` | `+2.85%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2459.668 ops/s` | `2786.565 ops/s` | `+13.29%` | `better` |
| `segment-index-persisted-mutation:putSync` | `440.510 ops/s` | `449.973 ops/s` | `+2.15%` | `neutral` |
