# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `50d4fb22290b1488dc4d28d3dee77f0398962149`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5161799.173 ops/s` | `4850217.131 ops/s` | `-6.04%` | `warning` |
| `segment-index-get-live:getMissSync` | `4724919.726 ops/s` | `4769799.995 ops/s` | `+0.95%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3546583.504 ops/s` | `3651601.908 ops/s` | `+2.96%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4626594.096 ops/s` | `4438931.387 ops/s` | `-4.06%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3647597.060 ops/s` | `3568806.636 ops/s` | `-2.16%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4662914.482 ops/s` | `4902266.071 ops/s` | `+5.13%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4224176.217 ops/s` | `4181727.418 ops/s` | `-1.00%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2191504.089 ops/s` | `2186346.287 ops/s` | `-0.24%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `549239.192 ops/s` | `560552.548 ops/s` | `+2.06%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `371670.329 ops/s` | `393656.173 ops/s` | `+5.92%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `177568.863 ops/s` | `166896.376 ops/s` | `-6.01%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `813740.977 ops/s` | `810862.261 ops/s` | `-0.35%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `799487.666 ops/s` | `797481.475 ops/s` | `-0.25%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14253.311 ops/s` | `13380.785 ops/s` | `-6.12%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6755.614 ops/s` | `6426.648 ops/s` | `-4.87%` | `warning` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6574.037 ops/s` | `6286.528 ops/s` | `-4.37%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2691.104 ops/s` | `2550.447 ops/s` | `-5.23%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2717.828 ops/s` | `2437.881 ops/s` | `-10.30%` | `worse` |
| `segment-index-range-scan:boundedScan` | `29.672 us/op` | `30.433 us/op` | `+2.57%` | `neutral` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2105.571 us/op` | `2114.790 us/op` | `+0.44%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4149.767 us/op` | `4091.696 us/op` | `-1.40%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `325.386 us/op` | `324.660 us/op` | `-0.22%` | `neutral` |
