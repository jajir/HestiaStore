# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `8a07667173e70d5b86a739f309a23e309b096fe7`
- Candidate SHA: `55a422c21e982146290a76d5d204f117017d8514`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2841611.514 ops/s` | `2755223.411 ops/s` | `-3.04%` | `warning` |
| `segment-index-get-live:getMissSync` | `4666763.787 ops/s` | `4749418.878 ops/s` | `+1.77%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `19385.948 ops/s` | `18257.319 ops/s` | `-5.82%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4541595.834 ops/s` | `4725339.226 ops/s` | `+4.05%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2616259.854 ops/s` | `2372124.977 ops/s` | `-9.33%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4804811.346 ops/s` | `4931814.471 ops/s` | `+2.64%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2588047.117 ops/s` | `2491387.114 ops/s` | `-3.73%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1279918.153 ops/s` | `1311977.882 ops/s` | `+2.50%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `322106.045 ops/s` | `331219.979 ops/s` | `+2.83%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `92419.560 ops/s` | `104167.401 ops/s` | `+12.71%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `229686.485 ops/s` | `227052.578 ops/s` | `-1.15%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `66080.586 ops/s` | `79564.930 ops/s` | `+20.41%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `60598.141 ops/s` | `73942.389 ops/s` | `+22.02%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5482.445 ops/s` | `5622.542 ops/s` | `+2.56%` | `neutral` |
