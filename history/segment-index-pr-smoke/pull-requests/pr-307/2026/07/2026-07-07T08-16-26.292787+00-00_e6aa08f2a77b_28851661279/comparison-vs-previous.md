# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `8df8b9848c086730320a4f5a276647c5586602a6`
- Candidate SHA: `e6aa08f2a77ba5b31cf4cf3f929c85a4fb1ebc41`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3026091.346 ops/s` | `2932143.142 ops/s` | `-3.10%` | `warning` |
| `segment-index-get-live:getMissSync` | `2814998.110 ops/s` | `2542165.699 ops/s` | `-9.69%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `2435320.233 ops/s` | `2606360.746 ops/s` | `+7.02%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2737710.290 ops/s` | `2781315.594 ops/s` | `+1.59%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2750149.703 ops/s` | `2815225.163 ops/s` | `+2.37%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1385904.700 ops/s` | `1397095.950 ops/s` | `+0.81%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `535636.430 ops/s` | `508181.054 ops/s` | `-5.13%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `308222.008 ops/s` | `285939.107 ops/s` | `-7.23%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `227414.422 ops/s` | `222241.947 ops/s` | `-2.27%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `851318.787 ops/s` | `882909.581 ops/s` | `+3.71%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `834389.027 ops/s` | `864868.678 ops/s` | `+3.65%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16929.760 ops/s` | `18040.903 ops/s` | `+6.56%` | `better` |
