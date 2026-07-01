# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e898a5940d69a8f46e99d82c0b4f48ffe78308c0`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2093338.942 ops/s` | `2012246.478 ops/s` | `-3.87%` | `warning` |
| `segment-index-get-live:getMissSync` | `2022963.953 ops/s` | `1795488.808 ops/s` | `-11.24%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `1405711.208 ops/s` | `1442171.017 ops/s` | `+2.59%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1826189.172 ops/s` | `1907113.585 ops/s` | `+4.43%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2048066.394 ops/s` | `2211535.809 ops/s` | `+7.98%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `936612.896 ops/s` | `1074525.793 ops/s` | `+14.72%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.855 ms/op` | `245.181 ms/op` | `+0.13%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `269.208 ms/op` | `268.249 ms/op` | `-0.36%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `242.561 ms/op` | `241.230 ms/op` | `-0.55%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `438833.471 ops/s` | `428669.769 ops/s` | `-2.32%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `199813.680 ops/s` | `191295.719 ops/s` | `-4.26%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `239019.791 ops/s` | `237374.051 ops/s` | `-0.69%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `925260.810 ops/s` | `899789.090 ops/s` | `-2.75%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `908820.375 ops/s` | `883065.015 ops/s` | `-2.83%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16440.435 ops/s` | `16724.076 ops/s` | `+1.73%` | `neutral` |
