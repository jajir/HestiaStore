# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6d8275b140b9735a85ba77e6dd5fd824362c5cc2`
- Candidate SHA: `8a07667173e70d5b86a739f309a23e309b096fe7`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2346568.828 ops/s` | `2303061.185 ops/s` | `-1.85%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3884519.143 ops/s` | `3917279.856 ops/s` | `+0.84%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `13637.718 ops/s` | `12486.354 ops/s` | `-8.44%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3582421.951 ops/s` | `3831191.006 ops/s` | `+6.94%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1635690.898 ops/s` | `1892354.437 ops/s` | `+15.69%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4186450.363 ops/s` | `3996048.531 ops/s` | `-4.55%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `1746266.615 ops/s` | `1738310.504 ops/s` | `-0.46%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1007870.252 ops/s` | `969723.411 ops/s` | `-3.78%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `290040.402 ops/s` | `273319.382 ops/s` | `-5.77%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `121835.781 ops/s` | `116202.060 ops/s` | `-4.62%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `168204.621 ops/s` | `157117.322 ops/s` | `-6.59%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `43462.899 ops/s` | `44613.854 ops/s` | `+2.65%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `38122.133 ops/s` | `39235.859 ops/s` | `+2.92%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5340.766 ops/s` | `5377.994 ops/s` | `+0.70%` | `neutral` |
