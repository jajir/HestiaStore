# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e898a5940d69a8f46e99d82c0b4f48ffe78308c0`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2229159.562 ops/s` | `2399001.549 ops/s` | `+7.62%` | `better` |
| `segment-index-get-live:getMissSync` | `2321989.597 ops/s` | `2292029.260 ops/s` | `-1.29%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1727116.013 ops/s` | `1983826.803 ops/s` | `+14.86%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1968819.834 ops/s` | `2053146.799 ops/s` | `+4.28%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2109792.828 ops/s` | `2099567.548 ops/s` | `-0.48%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1089694.030 ops/s` | `1084570.674 ops/s` | `-0.47%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `440663.326 ops/s` | `424692.815 ops/s` | `-3.62%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `290051.217 ops/s` | `269104.485 ops/s` | `-7.22%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `150612.109 ops/s` | `155588.330 ops/s` | `+3.30%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `599029.730 ops/s` | `542799.856 ops/s` | `-9.39%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `584738.051 ops/s` | `529806.614 ops/s` | `-9.39%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14291.679 ops/s` | `12993.243 ops/s` | `-9.09%` | `worse` |
