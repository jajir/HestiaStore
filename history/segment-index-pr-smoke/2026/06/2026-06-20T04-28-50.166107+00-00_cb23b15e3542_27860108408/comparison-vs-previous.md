# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `df14999b3b6143090d990872019097bf10ceffa6`
- Candidate SHA: `cb23b15e3542f2dcf618e20cb692494f55e16165`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2991267.747 ops/s` | `2802269.712 ops/s` | `-6.32%` | `warning` |
| `segment-index-get-live:getMissSync` | `2595883.423 ops/s` | `2839831.038 ops/s` | `+9.40%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2625404.860 ops/s` | `2487247.915 ops/s` | `-5.26%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `2634858.133 ops/s` | `2528020.101 ops/s` | `-4.05%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2742670.221 ops/s` | `2746738.408 ops/s` | `+0.15%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1420248.604 ops/s` | `1381895.722 ops/s` | `-2.70%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `363776.612 ops/s` | `343519.530 ops/s` | `-5.57%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `127955.200 ops/s` | `128399.434 ops/s` | `+0.35%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `235821.411 ops/s` | `215120.096 ops/s` | `-8.78%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `263406.139 ops/s` | `299015.450 ops/s` | `+13.52%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `244710.860 ops/s` | `281254.247 ops/s` | `+14.93%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18695.278 ops/s` | `17761.202 ops/s` | `-5.00%` | `warning` |
