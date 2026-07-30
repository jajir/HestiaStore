# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4c62ce188ffb07fdedd6c5d5d57d0453a33563a3`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5252688.435 ops/s` | `5161799.173 ops/s` | `-1.73%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4577894.037 ops/s` | `4724919.726 ops/s` | `+3.21%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3670365.349 ops/s` | `3546583.504 ops/s` | `-3.37%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4594084.324 ops/s` | `4626594.096 ops/s` | `+0.71%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3477225.556 ops/s` | `3647597.060 ops/s` | `+4.90%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4730186.487 ops/s` | `4662914.482 ops/s` | `-1.42%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4373983.761 ops/s` | `4224176.217 ops/s` | `-3.42%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2145929.533 ops/s` | `2191504.089 ops/s` | `+2.12%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `617366.993 ops/s` | `549239.192 ops/s` | `-11.04%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `437636.590 ops/s` | `371670.329 ops/s` | `-15.07%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `179730.403 ops/s` | `177568.863 ops/s` | `-1.20%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `786740.909 ops/s` | `813740.977 ops/s` | `+3.43%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `773205.884 ops/s` | `799487.666 ops/s` | `+3.40%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13535.024 ops/s` | `14253.311 ops/s` | `+5.31%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6665.623 ops/s` | `6755.614 ops/s` | `+1.35%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6674.729 ops/s` | `6574.037 ops/s` | `-1.51%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2786.508 ops/s` | `2691.104 ops/s` | `-3.42%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2702.672 ops/s` | `2717.828 ops/s` | `+0.56%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `34.010 us/op` | `29.672 us/op` | `-12.76%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2104.448 us/op` | `2105.571 us/op` | `+0.05%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4223.650 us/op` | `4149.767 us/op` | `-1.75%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `323.096 us/op` | `325.386 us/op` | `+0.71%` | `neutral` |
