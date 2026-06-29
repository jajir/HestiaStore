# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `22a358484ccacd047a48ac44106189a8adc50fc0`
- Candidate SHA: `ba315c9b6d99e9ef021ee4ca3f5ac8b78827fd14`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1924258.602 ops/s` | `1959324.577 ops/s` | `+1.82%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1952446.582 ops/s` | `1961836.073 ops/s` | `+0.48%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1871278.570 ops/s` | `1837053.390 ops/s` | `-1.83%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1859013.214 ops/s` | `1851985.020 ops/s` | `-0.38%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1997900.364 ops/s` | `1980161.351 ops/s` | `-0.89%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1045344.089 ops/s` | `1071979.168 ops/s` | `+2.55%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `368467.634 ops/s` | `357375.102 ops/s` | `-3.01%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `201990.541 ops/s` | `196742.040 ops/s` | `-2.60%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `166477.093 ops/s` | `160633.062 ops/s` | `-3.51%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `542733.343 ops/s` | `533970.009 ops/s` | `-1.61%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `528279.845 ops/s` | `517910.158 ops/s` | `-1.96%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14453.498 ops/s` | `16059.851 ops/s` | `+11.11%` | `better` |
