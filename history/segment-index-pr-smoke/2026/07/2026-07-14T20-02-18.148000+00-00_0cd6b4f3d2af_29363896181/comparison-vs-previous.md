# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6e6af75a8cc72508969ed93c52913c0d8b2a8916`
- Candidate SHA: `0cd6b4f3d2afb20ac2e4c8576adc66a7b6319770`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2233518.257 ops/s` | `2290375.922 ops/s` | `+2.55%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2253540.207 ops/s` | `2138419.657 ops/s` | `-5.11%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1969584.518 ops/s` | `1921893.030 ops/s` | `-2.42%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2099245.377 ops/s` | `2286245.901 ops/s` | `+8.91%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2213605.259 ops/s` | `2209971.374 ops/s` | `-0.16%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1133557.527 ops/s` | `1105473.502 ops/s` | `-2.48%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `491367.188 ops/s` | `466604.614 ops/s` | `-5.04%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `313827.600 ops/s` | `300023.232 ops/s` | `-4.40%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `177539.588 ops/s` | `166581.382 ops/s` | `-6.17%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `701514.351 ops/s` | `683883.605 ops/s` | `-2.51%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `686583.587 ops/s` | `667633.010 ops/s` | `-2.76%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14930.765 ops/s` | `16250.595 ops/s` | `+8.84%` | `better` |
